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
	Url  [][]string //分区存放链接
}
func NewPartition(nreduce int) *Partition{
	p :=  &Partition{
		NReduce: nreduce,
		Url:    make([][]string,nreduce),
	}
	return p
}

//定义任务的结构体
type Task struct {
	TaskId          string            //任务id
	WorksNums       uint64            //任务对应处理的WorksId
	Files           map[string]string //要处理的任务内容
	IsFinished      bool              //任务是否完成
	TaskTYpe        TaskType          //任务类型
	Partition       *Partition        //分区
	Expiration      int64             //任务的超时时间
	ConfirmTaskTime time.Duration     //确认收到时间

}

//给定任务id 和 工号 产生出一个Worker
func NewTask(nreduce int) *Task{
	np := NewPartition(nreduce)
	return &Task{
		TaskId:    uuid.NewV4().String() ,
		WorksNums:  0,
		Files:      make(map[string]string,0),
		IsFinished: false,
		TaskTYpe:   0,
		Partition: np,
		Expiration: 0,
		ConfirmTaskTime:time.Duration(time.Now().UnixNano()),
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
 	Version string                     //master 版本 todo 处理节点故障 每一次 新版本 就会进行一次备份master节点数据
}

// Worker 会不断RPC请求 获得要处理的 任务 如果没有获得任务就阻塞 等待任务
func (m *Master) GetTask(args Msg_Args, reply *Msg_Reply) error {
	m.lock.Lock()         //加锁
	defer m.lock.Unlock() //关闭锁
	//开始处理Woker请求
	fmt.Printf("Handler Work Request From Work ID : %d !\n", args.WorkNum)
	//如果Worker是新加入的机器 那么 咱们就给它 分配一个Worker编号
	if !args.Veify() {
		reply.Message = "Your Msg Is Expire or Invaild Please Resend It!\n"
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
			if len(v.Task)>0{
				reply.Message = "You Have To Finish The Task You Got Before!\n"
				reply.Invaild()
				reply.TaskType = UnfinishedTask //之前任务未完成
				return nil
			}
		}
	}
	//如果收到的 消息里已经有任务了 就说明 这条消息不是新建的 为了防止出现一些问题加上验证
	if len(reply.Tasks) >0 {
		reply.Message = "【Error】You send a invalid Reply,please ReInit A New Msg!\n"
		reply.Invaild()
		return nil
	}
	//循环从channel取 MapReduce任务
	for {
		select {
		case mt := <-m.MapTaskList:
			//给任务绑定员WorkNum
			mt.WorksNums = args.WorkNum
			mt.Expiration = time.Now().Add(time.Second * TASKTIMEOUT).UnixNano() //设置Task过期时间
			mt.TaskTYpe = MapTask                                                //设置task类型
			reply.Tasks = append(reply.Tasks, *mt)                               //给rpc返回
			//谁先抢到归谁 模式
			//将分配 出去的task存放在 work表里
			worktable, ok := m.worktable[args.WorkNum]
			worktable.Task[mt.TaskId] = *mt
			m.worktable[args.WorkNum] = worktable
			if MASTERDISTRIBUTIONMODEL == AVGDISTRIBUTION {
				if !ok {
					fmt.Println("【ERROR】Cannot Find WrkoInfo From Table!")
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
		case rt := <-m.ReduceTaskList:
			//Todo 按照规则指定Reduce生成的文件名字
			rt.TaskTYpe = ReduceTask
			rt.Expiration = time.Now().Add(time.Second * 60).UnixNano() //设置Task过期时间
			reply.Tasks = append(reply.Tasks, *rt)                      //给rpc返回
			reply.IsValid = true                                        //设置消息状态为true
			//将发出去的任务存储起来
			worktable, ok := m.worktable[args.WorkNum]
			if !ok {
				fmt.Println("【ERROR】Cannot Find WrkoInfo From Table!")
				return nil
			}
			worktable.Task[rt.TaskId] = *rt
			m.worktable[args.WorkNum] = worktable
			reply.TaskType = ReduceTask
			goto end
		default:
			//如果没有任务了
			if len(m.MapTaskList) == 0{
				//如果获取不到返回
				reply.Message = "No Task Get!\n"
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
	}else{
		fmt.Println("map handler。。。 todo")
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
			for _, work := range m.worktable {
				for i, task := range work.Task {
					//如果Worker过期了 把全部的任务 放回
					if work.isExpire() {
						switch task.TaskTYpe {
						case MapTask:
							task.WorksNums = 0
							x := work.Task[i]
							m.MapTaskList <- &x
							//从map中删除
							delete(work.Task, i)
							fmt.Printf("recycle task %s\n",task.TaskId )
						case ReduceTask:
							task.WorksNums = 0
							x := work.Task[i]
							m.MapTaskList <- &x
							//从map中删除
							delete(work.Task, i)
							fmt.Printf("recycle task %s\n",task.TaskId )
						}
					} else if task.Expired() {
						switch task.TaskTYpe {
						case MapTask:
							task.WorksNums = 0
							m.MapTaskList <- &task
							//从map中删除
							delete(work.Task, i)
							fmt.Printf("recycle task %s\n",task.TaskId )
						case ReduceTask:
							task.WorksNums = 0
							m.ReduceTaskList <- &task
							//从map中删除
							delete(work.Task, i)
							fmt.Printf("recycle task %s\n",task.TaskId )
						}
					}
				}

			}
			//扫描间隔
			time.Sleep(time.Second * 1)
		}
}

////从维护的 已投递任务中遍历 如果 taskid 和对应的 work匹配
////也就是说 如果 有个任务超时了 那么他的task 就分配给别人了
////那么就验证失败了
//func(m *Master) validTask(worknum uint64,taskid string) (bool,int){
//	for i,m := range m.DeliverTasks{
//		//如果任务id 和 workid 跟本地的投递任务表匹配
//		if m.TaskId == taskid && worknum == m.WorksNums {
//			//如果超时了 扔掉这条消息
//			if m.Expired() {
//				return false,-1
//			}
//			//返回 true 和索引
//			return true,i
//		}
//	}
//	return false,-1
//}
//
////任务完成时 Work 通过RPC调用 将任务ID告知
//func (m *Master) WorkeDoneInvock(args Msg_Args, reply *Msg_Reply) error {
//	fmt.Printf("Worker %d Finished reply Task args %s " ,args.WorkNum,args.TaskId)
//	//如果校验成功
//	if ok,index := m.validTask(args.WorkNum,args.TaskId);ok {
//		//判断要处理的任务类型
//		switch m.DeliverTasks[index].TaskTYpe  {
//			//处理完的Map任务
//			case MapTask:
//				//拿到值
//				val := m.DeliverTasks[index]
//				//从投递表中删除
//				m.DeliverTasks[index] = nil
//				//Map任务完成后扔到Reduce表
//				m.ReduceTaskList <- val
//			case ReduceTask:
//				//工作完成 设置为true
//				m.DeliverTasks[index].IsFinished = true
//		}
//	}
//	return nil
//}


//分割文件  按照每个文件不在超过一定的大小 比如 1M
func (m *Master) splitFiles(){
		for _,fl := range m.ToDoFils {
			//将文件分割成多个文件 并添加进列表
			m.splitFile(fl,m.SplitsSize)
		}
		fmt.Println("Split Files Finished!")
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
			fmt.Println(m,num,"tmplen",len(tmp))
			if num  == nMapLen {
				//新建一个任务
				ts := NewTask(nReduce)
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
			//遍历所有 worker节点
			for _,work := range m.worktable{
				//过期的Worker不加入计算
				if work.isExpire(){
					continue
				}
					//遍历work 里面的task 如果task没过期 计数 +1
					for _,task := range work.Task{
						if !task.Expired(){
							DeliverTaskNum ++
						}
					}

			}
			m.deliverTaskNum = DeliverTaskNum
			fmt.Printf("【INFO】Now Online Work Num : %d ,MapTask Num : %d ,Reduce Task Num : %d, OffLine Work Num : %d DeliverTaskNum: %d \n",
				len(m.worktable)-len(m.expireWork),len(m.MapTaskList),len(m.ReduceTaskList),len(m.expireWork),m.deliverTaskNum)
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
	if args.IsValid && !args.isExpire(){
		if _, ok := m.worktable[args.WorkId];ok{
			workinfo := m.worktable[args.WorkId]
			//跟新这个Worker 最后一次上线时间
			workinfo.TimeStamp = time.Duration(time.Now().UnixNano())
			//心跳次数
			workinfo.HeartNum += 1
			m.worktable[args.WorkId] = workinfo
			info := workinfo.ToString()
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


