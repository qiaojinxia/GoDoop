package src
/*
/ @Create by:CaomaoBoy on 2019-01-02
/ email:<1158829384@qq.com>
*/
import (
	"fmt"
	"github.com/satori/go.uuid"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


//分区结构
type Partition struct{
	NReduce int //分区数
	Url  []string //分区存放链接
}
func NewPartition(nreduce int) *Partition{
	p :=  &Partition{
		NReduce: nreduce,
		Url:    make([]string,nreduce),
	}
	return p
}

type taskstatus int8

const (
	TASKTYPE_IDLE = taskstatus(iota) //任务闲置	Master 生成任务时默认状态
	TASKTYPE_SENDING				//Master -> Worker任务发已发送 未确认
	TASKTYPE_DELIVERED               //任务已投递	Master -> Worker
	TASKTYPE_RECYLE                  //任务回收	Master

	TASKTYPE_UNCONFIRM               //Worker 任务未确认 Worker -> Master

	TASKTYPE_PROCESS 			//Worker 已确认未完成 Worker -> Master
	TASKTYPE_Finished  			//Worker 任务已完成未提交 Worker -> Master
	TASKTYPE_COMMITED                //Worker 任务已提交 未确认	Worker -> Master
	TASKTYPE_END                     //Worker Master 已提交 已确认	Worker -> Master

)

//定义任务的结构体
type Task struct {
	TaskId          string            //任务id
	WorksNums       uint64            //任务对应处理的WorksId
	Files           map[string]string //要处理的任务内容
	TaskTYpe        TaskType          //任务类型
	Partition       *Partition        //分区
	Ressign	string //记录返回结果的签名

	Status taskstatus //任务状态
	//如果任务发送过了Worker也会暂时保存这条信息
	// 如过Master 中途挂掉了 也会把消息返回
	//直到Master 过渡到下一个版本 或者设置的超时时间
	TaskSendExpire time.Duration //发送过的消息保留时间
	ConfireMasterVersion string //Master 版本

	Expiration      int64             //任务的超时时间
	ConfirmTaskTime int64       //确认收到时间

}

//查询是否可以更新状态 或者 更新状态
func (it *Task) UpdataTaskStaus(stataus taskstatus,updata ...bool) bool{
	statusstr := strings.Split("TASKTYPE_IDLE,TASKTYPE_SENDING,TASKTYPE_DELIVERED,TASKTYPE_RECYLE,TASKTYPE_UNCONFIRM,TASKTYPE_PROCESS,TASKTYPE_Finished,TASKTYPE_COMMITED,TASKTYPE_END",",")
	status := make(map[taskstatus][]taskstatus, 0)

	s1 := make([]taskstatus,0)
	s1 = append(s1, TASKTYPE_SENDING)
	status[TASKTYPE_IDLE] = s1
	s2 := make([]taskstatus,0)
	s2 = append(s2, TASKTYPE_DELIVERED)
	s2 = append(s2, TASKTYPE_RECYLE)
	s2 = append(s2, TASKTYPE_IDLE)
	s2 = append(s2, TASKTYPE_PROCESS)
	status[TASKTYPE_SENDING] = s2
	s3 := make([]taskstatus,0)
	s3 = append(s3, TASKTYPE_RECYLE)
	s3 = append(s3, TASKTYPE_IDLE)
	s3 = append(s3, TASKTYPE_Finished)
	s3 = append(s3, TASKTYPE_UNCONFIRM)
	s3 = append(s3, TASKTYPE_END)
	status[TASKTYPE_DELIVERED] = s3


	s4 := make([]taskstatus,0)
	s4 = append(s4, TASKTYPE_PROCESS)
	status[TASKTYPE_UNCONFIRM] = s4

	s5 := make([]taskstatus,0)
	s5 = append(s5, TASKTYPE_COMMITED)
	s5 = append(s5, TASKTYPE_Finished)
	status[TASKTYPE_PROCESS] = s5

	s6 := make([]taskstatus,0)
	s6 = append(s6, TASKTYPE_END)
	status[TASKTYPE_COMMITED] = s6

	s7 := make([]taskstatus,0)
	s7 = append(s7, TASKTYPE_COMMITED)
	status[TASKTYPE_Finished] = s7

	//如果提交的 状态 是当前状态的下一个状态
	if updata[0] == false{
		for _,v := range status[it.Status]{
			if v == stataus{
				Info.Printf("Status %s Can Invert To Status %s",statusstr[it.Status],statusstr[stataus])
				return true
			}
		}
	}else if  updata[0] == true{
		for _,v := range status[it.Status]{
			if v == stataus{
				Info.Printf("Status %s Can Invert To Status %s",statusstr[it.Status],statusstr[stataus])
				//更新成下一个状态
				it.Status = stataus
				return true
			}
		}
	}
	Error.Printf("Status %s Can't Invert To Status %s",statusstr[it.Status],statusstr[stataus])
	return false
}

//确认超时
func (it *Task) IsConfirmOutTime() bool{
	if it.ConfirmTaskTime == 0 {
		Error.Println("Msg Error!")
		return true
	}
	//如果 当前 时间 大于过期时间 则 过期
	return time.Now().UnixNano() > it.ConfirmTaskTime
}

//给定任务id 和 工号 产生出一个Worker
func NewTask(nreduce int,tstatus taskstatus) *Task{
	np := NewPartition(nreduce)
	return &Task{
		TaskId:    uuid.NewV4().String() ,
		WorksNums:  0,
		Files:      make(map[string]string,0),
		TaskTYpe:   0,
		Partition: np,
		Expiration: 0,
		ConfirmTaskTime:0,
		Ressign:"",
		Status:tstatus,
	}
}

//如果这个任务超过处理时间 就分配给其他的Worker
func (it Task) Expired() bool{
	if it.Expiration == 0 {
		return false
	}
	//如果 当前 时间 大于过期时间 则 过期
	return time.Now().UnixNano() > it.Expiration
}

const VERSIONEXPIRTIONTIME  = time.Hour * 2 //2小时内无法完成这批任务全部放弃 
//控制任务版本 
type VerionInfo struct {
	versionid int //版本号
	versionname string	//版本名
	expiretime time.Duration//版本过期时间
	prv *Master //上一个版本
	next *Master //下一个版本
}

func NewVersionInfo(versionid int,versionname string) *VerionInfo{
	a :=  &VerionInfo{
		versionid:   versionid,
		versionname: versionname,
		expiretime:  0,
	}
	a.expiretime = time.Duration(time.Now().Add(VERSIONEXPIRTIONTIME).UnixNano())
	return a
}
//版本是否过期
func (vif VerionInfo) VersionIsExpire() bool{
	if int64(vif.expiretime) < time.Now().UnixNano(){
		return true
	}
	return false
}

type Master struct {
	// Your definitions here.
	lock           sync.Mutex
	ToDoFils       []string            //待处理的文件
	taskNum        uint64              //总任务数 包含 Reduce  和 Map 任务
	MapTaskList    chan  *Task         //所有分配出去未完成的Map任务表 当任务完成后将会转变为ReduceTask
 	ReduceTaskList chan *Task          //上面的表任务完成后将会转变为当前表的任务
 	nREduce        int                 //要分割成多少个中间文件
	worktable      map[uint64]WorkInfo //worker池
 	MSplits        []string            //分割后的文件列表
 	SplitsSize     int                 //将文件分割成的大小
 	nMap           int                 //每个Worker 能拿到多少个 分割后的文件
	deliverTaskNum int                 //投递出去任务数量
 	expireWork []uint64                //超时Workid
 	exitChan chan bool                 //是否关闭master

	rwm sync.RWMutex //读写保护锁
 	versionInfo VerionInfo              //master 版本 todo 处理节点故障 每一次 新版本 就会进行一次备份master节点数据

}

// Worker 会不断RPC请求 获得要处理的 任务 如果没有获得任务就阻塞 等待任务
func (m *Master) GetTask(args Msg_Args, reply *Msg_Reply) error {
	m.lock.Lock()         //多个线程请求加锁 防止出错
	defer m.lock.Unlock() //关闭锁
	//开始处理Woker请求
	fmt.Printf("Handler Work Request From Work ID : %d !\n", args.WorkNum)
	//如果Worker是新加入的机器 那么 咱们就给它 分配一个Worker编号
	if !args.Veify() {
		reply.Message = "Your Msg Is Expire or Invaild Please Resend It!\n"
		reply.Invaild()
		return nil
	}
	//如果收到的 消息里已经有任务了 就说明 这条消息不是新建的 为了防止出现一些问题加上验证
	if len(reply.Tasks) >0 {
		reply.Message = "【Error】You send a invalid Reply,please ReInit A New Msg!\n"
		reply.Invaild()
		return nil
	}
	if args.WorkNum == InitWorkNum {
		reply.Message = "Worker ID Is Not Registribution Please Register First!\n"
		reply.Invaild()
		return nil
		//如果Worker 不是新机器 那么 我们看看在Worker登记表里看看有没有这个Worker
	} else {
		//遍历工人登记表
		v, ok := m.worktable[args.WorkNum]
		if !ok {
			reply.Message = "Worker ID Is Not Exists Please Register First!\n"
			reply.Invaild()
			return nil
			if time.Now().UnixNano() > int64(v.TimeStamp) {
				reply.Message = "Worker ID Is  Expire Please Register First!\n"
				reply.Invaild()
				fmt.Printf("Receiver A Get Task Request But Workid Id  Out of Contact !\n")
				return nil
			}
		}

		//是否验证完成之前的任务后 才能再次得到任务
		if CANDEBETGETTASK != -1{
			//Worker 最大拥有任务数
			if len(v.Task) >= OWNTASKNUM {
				reply.Message = "You Have To Finish The Task You Got Before!\n"
				reply.Invaild()
				reply.TaskType = UnfinishedTask //之前任务未完成
				return nil
			}
		}
	}
	//循环从channel取 MapReduce任务
	for {
		select {
		//从管道中 取任务
		case mt := <-m.MapTaskList:
			//对 已经生成的任务 进行 分配前的准备
			mt.WorksNums = args.WorkNum
			err := mt.UpdataTaskStaus(TASKTYPE_SENDING,true)
			//如果任务状态无法转换
			if err == false {
				panic("error")
			}
			mt.Expiration = time.Now().Add(time.Second * TASKTIMEOUT).UnixNano() //设置Task过期时间
			mt.TaskTYpe = MapTask //设置task类型
			mt.ConfirmTaskTime = time.Now().Add(time.Second * CONFIRMOUTTIME).UnixNano()//设置确认超时时间
			//---------------
			reply.Tasks = append(reply.Tasks, *mt)  //给rpc返回
			//谁先抢到归谁 模式
			//将分配 出去的task存放在 work表里
			worktable, ok := m.worktable[args.WorkNum]
			worktable.Task[mt.TaskId] = *mt
			m.worktable[args.WorkNum] = worktable
			if MASTERDISTRIBUTIONMODEL == AVGDISTRIBUTION {
				if !ok {
					Error.Printf("Cannot Find WrkoInfo WorkNum %d From Table!",args.WorkNum)
				}
				reply.TaskType = MapTask
				goto end
			} else if (MASTERDISTRIBUTIONMODEL == MAXONEWORKER) {
				//抢占模式 只要管道里 有的全部得到
				if  OWNMAX ==0 {
					if len(m.MapTaskList) ==0{
						reply.TaskType = MapTask
						goto end
					}
					//其他模式 这时候按照指定数量获取
				}else{
					//如果 已经获得了 OWNMAX指定的数量任务 或者 管道里没有任务了 就返回
					if(len(reply.Tasks) >= OWNMAX || len(m.MapTaskList) == 0 ){
						reply.TaskType = MapTask
						goto end
					}
				}
			}else if MASTERDISTRIBUTIONMODEL == AUTODISTRIBUTION {
				//通过计算 每台计算机时间 处理返回时 和文件大小算出算力 按照 算力 * 文件总数 来动态的根据性能分配
				fmt.Println("todo")
			}
		//case rt := <-m.ReduceTaskList:
		//	//Todo 按照规则指定Reduce生成的文件名字
		//	rt.TaskTYpe = ReduceTask
		//	rt.UpdataTaskStaus(TASKTYPE_SENDING,false)
		//	rt.Expiration = time.Now().Add(time.Second * 60).UnixNano() //设置Task过期时间
		//	reply.Tasks = append(reply.Tasks, *rt)                      //给rpc返回
		//	reply.IsValid = true                                        //设置消息状态为true
		//	//将发出去的任务存储起来
		//	worktable, ok := m.worktable[args.WorkNum]
		//	if !ok {
		//		fmt.Println("【ERROR】Cannot Find WrkoInfo From Table!")
		//		return nil
		//	}
		//	worktable.Task[rt.TaskId] = *rt
		//	m.worktable[args.WorkNum] = worktable
		//	reply.TaskType = ReduceTask
		//	goto end
		default:
			//如果没有任务了
			if len(m.MapTaskList) == 0{
				//如果获取不到返回
				reply.Message = "Now Has No Task In Master!\n"
				reply.TaskType = NoneTask
				reply.Sign()
				return nil
			}
		}
	}
	end:
	tasktype := "MapTask"
	if reply.TaskType == ReduceTask {
		tasktype ="ReduceTask"
	}
	reply.Sign()
	taskids:="["
	//格式化输出id
	for i,m := range reply.Tasks {
		if i == len(reply.Tasks) -1 {
			taskids += "\""+ m.TaskId +"\""+ "]"
		}else{
			taskids += "\""+ m.TaskId +"\"" +","
		}
	}
	reply.Message = "【INFO】You Receiver "+ string(len(reply.Tasks))+ tasktype + " Task!\n"
	fmt.Printf("【INFO】Reply %s Messages  Will Deliver To Work :%d ,TasksNum :%d Taskids :%s , TaskType :%s !\n",tasktype,
		reply.WorkNum,len(reply.Tasks),taskids,tasktype)
	return nil
}

//维护已经分配出去的任务
func(m *Master) delivertaskscan() {
		for {
			//遍历Work表
			m.rwm.Lock()
			for _, work := range m.worktable {
				for i, task := range work.Task {
					//如果Worker过期了 把全部的任务 放回
					if work.isExpire() {
						switch task.TaskTYpe {
						case MapTask:
							task.WorksNums = 0
							x := work.Task[i]
							up := x.UpdataTaskStaus(TASKTYPE_RECYLE,true)
							if up == false{
								continue
							}
							m.MapTaskList <- &x
							//从map中删除
							delete(work.Task, i)
							fmt.Printf("Recycle TaskResult %s\n",task.TaskId )
						case ReduceTask:
							task.WorksNums = 0
							x := work.Task[i]
							up := x.UpdataTaskStaus(TASKTYPE_RECYLE,true)
							if up == false{
								continue
							}
							m.MapTaskList <- &x
							//从map中删除
							delete(work.Task, i)
							Info.Printf("Recycle TaskResult %s\n",task.TaskId )
						}
						//任务 过期或者任务超时 回收任务
					} else if task.Expired() || task.IsConfirmOutTime() {
						//完成的任务不需要维护
						if task.Status != TASKTYPE_END {
							switch task.TaskTYpe {
							case MapTask:
								task.WorksNums = 0
								up := task.UpdataTaskStaus(TASKTYPE_RECYLE,true)
								if up == false{
									continue
								}
								m.MapTaskList <- &task
								//从map中删除
								delete(work.Task, i)
								fmt.Printf("recycle TaskResult %s\n",task.TaskId )
							case ReduceTask:
								task.WorksNums = 0
								up := task.UpdataTaskStaus(TASKTYPE_RECYLE,true)
								if up == false{
									continue
								}
								m.ReduceTaskList <- &task
								//从map中删除
								delete(work.Task, i)
								fmt.Printf("recycle TaskResult %s\n",task.TaskId )
							}
						}
					}
				}

			}
			m.rwm.Unlock()
			//扫描间隔
			time.Sleep(time.Second * 1)
		}
}

//分割文件  按照每个文件不在超过一定的大小 比如 1M
func (m *Master) splitFiles(){
		for _,fl := range m.ToDoFils {
			//将文件分割成多个文件 并添加进列表
			m.splitFile(fl,m.SplitsSize)
		}
		fmt.Println("Split Files Finished!")
}
//生成Reduce任务
func (m *Master)GenerateReduce(){
	for{
		time.Sleep(time.Second * 1)
		var worklfinished map[string]Task
		ts := NewTask(m.nREduce,TASKTYPE_IDLE)
		//遍历work表找到 Map类型的 并且已经完成的任务
		taskcontent := make(map[string]string,0)
		for i,wk := range m.worktable{
			for _,tsk := range wk.Task{
				if tsk.Status == TASKTYPE_END && tsk.TaskTYpe == MapTask{
					if len(wk.FinishedTask) == 0{
						worklfinished = make(map[string]Task,0)
					}else{
						worklfinished = wk.FinishedTask
					}
					worklfinished[tsk.TaskId] = tsk

				}
				delete(wk.Task,tsk.TaskId)
			}
			if worklfinished != nil{
				wk.FinishedTask = worklfinished
				m.worktable[i] = wk
			}
		}
		for i:=0;i<m.nREduce;i++{
			for i,wk := range m.worktable{
				for _,ftsk := range wk.FinishedTask{
					//存储的url 为本地 todo 后期改成服务器地址
					taskcontent[ftsk.Partition.Url[i]]=ftsk.Partition.Url[i]
				}

			}
		}
		if len(taskcontent) > 0 {
			ts.Files = taskcontent
			m.ReduceTaskList <- ts
		}
	}

///endXXXXXXXXXXXXXXXXXXXX
}

//生成待分配的任务
func (m *Master) generateTask(){
		fmt.Println("Start Generate Tasks!","splitsfile:",len(m.MSplits))
		//生成任务
		num := 0 //用来处理 每任务能获取的 splits数
		tmp := make(map[string]string)//存储临时添加的文件 nmap指定 每个task能得到几个待处理文件
		var tasks chan *Task //存储任务
		nMapLen := m.nMap
		tasks = m.MapTaskList
		var nReduce = m.nREduce //方便for循环内部访问
		//循环遍历待处理的文件
		for _,m := range m.MSplits{
			num ++//计次
			//todo 可以存储url suchas ["cat.txt":"wwww.baidu.com/cat.txt"]
			tmp[m] = m
			if num  == nMapLen {
				//新建一个任务
				ts := NewTask(nReduce,TASKTYPE_IDLE)
				for k,_ := range tmp {
					//将待处理的files 添加进 Tasks
					ts.Files [k] = k
				}
				tmp = make(map[string]string)//清空
				tasks  <- ts//往channel扔task
				num = 0//清零
			}
		}
		//将任务添加进 master的Server
		m.MapTaskList = tasks
}

//分割文件 输出格式 文件名.txt0  文件名.txt1 。。。
func(m *Master) splitFile(path string,size int) {
	file, err:= os.Open(path)
	defer file.Close()
	if err != nil {
		fmt.Println("Failed To Documents:", *file)
		return
	}
	finfo, err := file.Stat()
	if err != nil {
		fmt.Println("Get Documents Info Failed", file, size)
		return
	}
	//打印文件信息
	fmt.Printf("Get Documents Info  Nname:%s ,ModifyTime:%s ,FileSize:%d kb \n",finfo.Name(),finfo.ModTime(),finfo.Size())
	bufsize := 2 << 19 // 默认 1024 * 1024 1M大小
	//如果小于 1M那么 就为要读文件的大小
	if size < bufsize {
		bufsize = size
	}
	//定义读取的 容器
	buf := make([]byte, bufsize)
	num := (int(finfo.Size()) + size - 1) / size //计算分块数量
	fmt.Printf("Documents Will Be Divided Into %d Copies!\n",num)
	for i := 0; i < num; i++ {
		copylen := 0
		//文件名.txt + 块编号{0}
		newfilename := finfo.Name() + strconv.Itoa(i)
		newfile, err1 := os.Create(newfilename)
		if err1 != nil {
			fmt.Println("Failed To Create Documents!", newfilename)
		} else {
			fmt.Println("Create Documents:", newfilename)
		}
		for copylen < size {
			n, err2 := file.Read(buf)
			if err2 != nil && err2 != io.EOF {
				fmt.Println(err2, "Failed To Read From:", file)
				break
			}
			if n <= 0 {
				break
			}
			w_buf := buf[:n]
			newfile.Write(w_buf)
			copylen += n
		}
		//将分割后的文件名添加进列表
		m.MSplits = append(m.MSplits, newfilename)
	}
	return
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) Server() {
	//将文件进行分割
	m.splitFiles() //分割文件
	m.generateTask() //生成任务
	go m.delivertaskscan()//维护已经分配出去的任务
	go m.sacnExpireWork()
	go m.showInfo()
	go m.GenerateReduce()
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	//l, e := net.Listen("UnixNano", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	m.Done()
}

//扫描过期的Worker
func (m *Master) sacnExpireWork(){
	for{
		select {

		case isexit :=<- m.exitChan:
			m.exitChan <- isexit
			if isexit{
				goto scanend
			}
		default:
			m.rwm.Lock()
			newlist:= make([]uint64,0)
			for id,wk := range m.worktable{
				//如果Worker 过期了 那就加入过期表
				if wk.isExpire(){
					newlist = append(newlist,id)
				}
			}
			//如果超过最大过期时间 将这个节点直接移除
			for i,workid := range newlist {
				if workid == 0{
					continue
				}

				//如果找到了 这个过期的WorkInfo
				if work,ok := m.worktable[workid];ok{
					if !work.isExpire(){
						newlist[i] = 0
						//如果超过了最大过期时间
					}else if work.isExpireMax(){
						//从过期列表删除
						newlist[i] = 0
						//删除这个map内容
						delete(m.worktable,workid)
					}
				}
				m.rwm.Unlock()
				tmplist := make([]uint64,0)
				for _,workid := range newlist {
					if workid == 0 {
						continue
					}
					tmplist = append(tmplist, workid)
				}
				m.expireWork = tmplist

			}
			time.Sleep(time.Second * 1)
		}
	}
	scanend:
}

func (m *Master) showInfo() {
	for{
		select {
			case isexit :=<- m.exitChan:
				m.exitChan <- isexit
				if isexit{
					goto infoend
				}
		default:
			DeliverTaskNum := 0
			mapFinished := 0
			reduceFinished := 0
			sendingtask := 0
			//遍历所有 worker节点
			for _,work := range m.worktable{
				//遍历work 任务表
				for _,task := range work.Task{
					//过期的WOrker 未完成的不计算在内
					if work.isExpire() {
						break
					}
					if !task.Expired() && task.Status == TASKTYPE_DELIVERED {
						DeliverTaskNum ++
					}
					if !task.Expired() && task.Status == TASKTYPE_SENDING {
						sendingtask ++
					}
				}
					//遍历work 里面的完成任务列表
				for _,task := range work.FinishedTask{
					if task.Status == TASKTYPE_END {
						if task.TaskTYpe == MapTask{
							//生成Reduce任务
							mapFinished += 1
						}else if task.TaskTYpe == ReduceTask{
							reduceFinished +=1
						}
					}
				}

			}
			m.deliverTaskNum = DeliverTaskNum
			fmt.Printf("【INFO】Now Online Work Num : %d ,MapTask Num : %d ,Reduce Task Num : %d, " +
				"OffLine Work Num : %d DeliverTaskNum: %d SendingTaskNum: %d  MapFinished: %d ReduceFinished: %d \n",
				len(m.worktable)-len(m.expireWork),len(m.MapTaskList),len(m.ReduceTaskList),len(m.expireWork),m.deliverTaskNum,sendingtask,mapFinished,reduceFinished)
			time.Sleep(time.Second * 2)
		}
	}
	infoend:

}

func (m *Master) RegisterWorker(args Msg_Regs,reply *Reply_Regs) error{
	fmt.Printf("Receiver a Worker Request Work ID: %d \n",args.WorkID)
			if args.Verify(){
				//判断这个id有没有注册过
				if _, ok := m.worktable[args.WorkID];ok{
					fmt.Printf("A Worker ReOnline User WorkID :%d ,WorkInfo: %v \n",args.WorkID,args.WorkData)
				}
				if args.WorkID == InitWorkNum {
					reply.WorkID = uint64(len(m.worktable) + 1)
				}
				//将Works信息 添加到当 Works列表里
				m.worktable[reply.WorkID] = *args.WorkData
				fmt.Printf("Regs Work Sucess! Work ID : %d Work Info %s \n",reply.WorkID,(*args.WorkData).ToString())
				reply.Sign()
			}
			return nil
}

//监听心跳
func (m *Master) HeartMonitor(args Msg_Heart,reply *Reply_Heart) error{
	//加锁
	if args.IsValid && !args.isExpire(){
		if _, ok := m.worktable[args.WorkId];ok{
			workinfo := m.worktable[args.WorkId]
			//跟新这个Worker 最后一次上线时间
			workinfo.TimeStamp = time.Duration(time.Now().UnixNano())
			//心跳次数
			workinfo.HeartNum += 1
			m.worktable[args.WorkId] = workinfo
			info := workinfo.ToString()
			//如果心跳中包含 节点收到Task的确认消息
			if args.hasTaskInfo(){
				rtinfo := make([]string,0,len(args.ConfimTaskID))
				for i:=0;i<len(args.ConfimTaskID);i++{
					taskid := args.ConfimTaskID[i]
					//校验 当前Work的这个任务是否存在 存在的进行 记录
					task,ok :=  m.worktable[args.WorkId].Task[taskid]
					if !ok{
						continue
					}
					//改为已确认投递状态
					up := task.UpdataTaskStaus(TASKTYPE_DELIVERED,true)
					if up == false{
						continue
					}
					//如果能在本地表里找到 设置确认时间 然后确认给Worker
					rtinfo = append(rtinfo, args.ConfimTaskID[i])
					//收到确认消息
					task.ConfirmTaskTime = time.Now().Add(time.Hour * 9999).UnixNano()
					//设置返回的消息
					reply.AckConfirm = rtinfo
					m.worktable[args.WorkId].Task[taskid] = task
				}
			}
			//如果心跳中包含 完成的任务
			if args.hasTaskFinished(){
				for i:=0;i<len(args.TaskResult);i++{
					//在本地表中找到task
					taskid := args.TaskResult[i].TaskId
					task,ok :=  m.worktable[args.WorkId].Task[taskid]
					if !ok{
						continue
					}
					//确认任务任务没有过期
					if task.Expired() != true{
						fin := make([]string,0,len(args.TaskResult))

						//验证 worid 与 本地信息相符
						if task.WorksNums == args.WorkId && task.TaskTYpe == args.TaskResult[i].TaskType {
							//把 收到的url信息 全部记录到本地
								if args.TaskResult[i].getSign() == args.TaskResult[i].sign || task.Ressign == args.TaskResult[i].sign{
									Info.Printf("收到了完成的任务 %v",args.TaskResult[i].PathUrl)
									//将本地的par修改成 Worker的 包含了 文件的地址
									task.Partition= &args.TaskResult[i].PathUrl
									//改为任务已完成状态
									up := task.UpdataTaskStaus(TASKTYPE_END,true)
									if up == false{
										continue
									}
									m.worktable[args.WorkId].Task[taskid] = task
									//将任务id放进去人列表
									fin = append(fin, task.TaskId)

								}else{
									Info.Println("Message sign is invalid or this Message has deliver!")
								}
						}
					}
				}
			}
			
			fmt.Printf("Work Id %d Send Heart  Update Workinfo Now Is %s \n",args.WorkId,info)
			reply.Sign()
		}else{
			fmt.Printf("Workid %d is not exists!",args.WorkId)
		}

	}else{
		fmt.Printf("Waning: Get a Expire Heart Message From WorkId: %d ! ",args.WorkId)
		return nil
	}
	return nil
}
//监听master 退出状态
func(m *Master) Done(){
	for{
		select {
			case exit :=<- m.exitChan:
				if exit {
					goto endserver
				}
		}
	}
	//结束服务
	endserver:
}


// 创建Master节点
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		ToDoFils:       files,
		worktable:       make(map[uint64]WorkInfo,0),
		taskNum:        0,
		MapTaskList:    make(chan *Task,1024), //存放MapTask channel todo 大小设置问题
		ReduceTaskList: make(chan *Task,1024),//存放ReduceTask channel todo 大小设置问题
		nREduce:nReduce,
		MSplits:make([]string,0),
		expireWork:make([]uint64,0),
		deliverTaskNum:0,
		SplitsSize: 1024 * 512, //512kb
		nMap:2,
	}
	fmt.Println("Start MapReduce Server!")
	// Your code here.
	return &m
}


