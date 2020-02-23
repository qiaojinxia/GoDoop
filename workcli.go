package src
/*
/ @Create by:CaomaoBoy on 2019-01-13
/ email:<1158829384@qq.com>
*/
import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

func DefaultWirteOutBuff (sh *storeHandler,funtype int,par uint32,kv *string) {
	switch funtype {
	case HANDLER:
		sh.Handler(par,kv)
	case MERGEFILE:
		sh.MergeFile()
	case WRITEOUTBUFF:
		sh.WriteOutBuff()

	}
}
type WorkerCli struct {
	lock          sync.Mutex
	WorkID        uint64          //工人的编号
	IsRegist      bool            //是否注册
	TaskTable     map[string]Task //任务表
	fileStream    chan []byte     //读取文件管道
	isFinished    chan bool       //判断有没有读取完数据
	mapf          func(string, string) []KeyValue
	reducef       func(string, []string) string
	heartInterval time.Duration //心跳间隔
	heartNexttime time.Duration //下次发送心跳时间
	heartCount uint64           //心跳次数
	tryouttime int              //超时重试次数
	alive chan bool             //决定着 Worker是否罢工
	workdata *WorkInfo          //记录worker信息
	workFinished chan TaskPack	//存放完成的任务 需要通过心跳返还给Master
	confirmTask []string        //如果Worker 收到了这个 任务就会添加到列表  通过心跳返回给Master 这样Master 就会确认你收到了 模仿tcp/ip的三次握手
	isConfirm []string        //如果Worker 收到了这个 任务就会添加到列表  通过心跳返回给Master 这样Master 就会确认你收到了 模仿tcp/ip的三次握手
	rb *CacheBuf                //缓冲读取数据 排序 写出
	nReduce	uint32			//划分任务数
	hearflg		bool	//心跳标志位


}

//初始化 Worker客户端
func NewWorkerCli(nReduce uint32,mapf func(string, string) []KeyValue,reducef func(string, []string) string) *WorkerCli{
	a:= &WorkerCli{
		WorkID:        InitWorkNum,
		IsRegist:      false,
		TaskTable:     nil,
		fileStream:    make(chan []byte,1024),
		isFinished:    make(chan bool,1),
		mapf:          mapf,
		reducef:       reducef,
		heartInterval: time.Second * 10,
		heartNexttime: time.Duration(time.Now().UnixNano()),
		alive:         make(chan bool,1),
		rb:            NewCacheBuf(nReduce ,1024 * 1024 *10),
		workFinished:make(chan TaskPack,24),
		isConfirm:make([]string,0,10),
		nReduce:nReduce,
		hearflg:false,
	}
	a.alive <- false//初始默认节点处于离线状态
	//设置写出的处理函数
	a.rb.SetCacheBuffWriteFunc(DefaultWirteOutBuff)
	return a
}


//主动获取任务
func (wc *WorkerCli) Working() error{
		isalive := wc.isalive()
		if isalive == false{
			Info.Println("Work Now Is Offline Waiting To Register!")
			return errors.New("Work Offline")
		}
		Info.Printf("Worker Id %d Will Request Get Tak!\n",wc.WorkID)
		//初始化消息参数
		reply := NewMsg_Reply(wc.WorkID)
		args := NewMsg_Args(wc.WorkID, MapTask)
		//调用用获取任务
		args.Sign()
		wc.call(RPC_TASKDISTRIBUTION,args,reply)
		Info.Print("Master Return Info:",reply.Message)
		//存在之前没完成的任务
		if reply.TaskType == UnfinishedTask {
			Info.Printf("Waiting to finished my work first!")
			//todo 要么放弃 要么存储重新提交
			finished := 0
			for _,v := range wc.TaskTable{
				//未提交 且 未过期的任务
				if v.Status != TASKTYPE_COMMITED && !v.Expired(){
					finished += 1
				}
			}
			return errors.New(fmt.Sprintf("Task Beyond what you have not finished %d Max you can own %d\n",finished,OWNTASKNUM))
		}
		if !reply.Verify(){
			return errors.New("Reply Message Invalid!")
		}
		wc.hearflg = false
		//将受到的确认 收到消息放回到列表 用于心跳时传回
		for _,m := range reply.Tasks{
			wc.confirmTask = append(wc.confirmTask, m.TaskId)
		}
		retrytimes :=0
		for !wc.hearflg {
			retrytimes +=1
			//等待信条
			Info.Printf("Has %d Task Waiting to Heart Confirm!",len(wc.confirmTask))
			time.Sleep(time.Second * 1)
			//超过20秒 放弃这次Worker请求的内容
			if retrytimes > 20{
				return errors.New("Confirm Retrytimes Outtimes !")
			}
		}
		//将收到的任务放进Task表中 去除没有确认的
		for _,tsk := range reply.Tasks{
			Info.Printf("ADD Task %s To  TaskList !",tsk.TaskId)
			if Contains(wc.isConfirm,tsk.TaskId) {
				//如果确认任务 设置为处理中
				up := tsk.UpdataTaskStaus(TASKTYPE_PROCESS,true)
				if up == false{
					panic("status error")
				}
			}else{
				//未完成
				tsk.UpdataTaskStaus(TASKTYPE_UNCONFIRM,true)
			}
			//如果task表没有初始化
			if len(wc.TaskTable) == 0 {
				wc.TaskTable =  make(map[string]Task,0)
			}
			wc.TaskTable[tsk.TaskId] = tsk
		}
		//遍历收到的所有Task
		for i:=0;i< len(reply.Tasks);i++ {
			//初始化写出的方法
			sh := NewStoreHandler(wc.nReduce,reply.Tasks[i].TaskId)
			wc.rb.SetStoreHandler(sh)
			switch reply.Tasks[i].TaskTYpe {
			case MapTask:
				tmptable := wc.TaskTable[reply.Tasks[i].TaskId]
				wc.doMapTask(args,reply,i)
				//如果写出成功 bingqie url表大于0
				if sh.successwrite && len(sh.url) > 0{
					up := tmptable.UpdataTaskStaus(TASKTYPE_Finished,true)
					if up == false{
						panic("error")
					}
					//将文件路径  放到url
					var urls []string
					if len(tmptable.Partition.Url[i]) <len(sh.url){
						urls = make([]string,len(sh.url))
					}else{
						urls = tmptable.Partition.Url
					}
					for i,v := range sh.url{
						//将写出处理器里面的 数据保存到 任务表里面 另一个协程将会 负责回传消息给Master
						urls[i] = v
					}
					//设置返回任务完成的消息
					tmptable.Partition.Url= urls
					wc.TaskTable[reply.Tasks[i].TaskId] = tmptable
				}
			case ReduceTask:
				wc.doReduceTask(args,reply,i)
			default:
				return errors.New("Unkown error!")
			}
		}
	return nil
}
func (wc *WorkerCli) doMapTask(args Msg_Args,reply *Msg_Reply,taskid int){
	//根据任务数初始化管道数目
	go wc.ReadFile(reply,taskid)
	Info.Println("Prepare complete  TaskResult will run....")
	//携程处理文件读写
	wg := sync.WaitGroup{}
	wg.Add(1)
	go wc.CoreHandler(&wg)
	//等待处理完毕
	wg.Wait()

}

func(wc *WorkerCli) CoreHandler(wg *sync.WaitGroup){
	Info.Println("Start Read from chan")
	//初始化ringbuff
	for{
		select{
			case data := <- wc.fileStream:
				entry := wc.mapf("xx", string(data))
				for _,v := range entry{
					//写入环形缓冲区
					wc.rb.Collect(v)
				}
			//判断有没有读取完成
			default:
				if len(wc.fileStream) == 0{
					if len(wc.isFinished) != 0{
						complete :=<- wc.isFinished
						if complete == true{
							wc.rb.SpillAll()
							goto end
						}
					}

				}
		}
	}
	end:
	wg.Done()
	Info.Println("TaskResult finished!")
}

func (wc *WorkerCli) doReduceTask(args Msg_Args,reply *Msg_Reply,taskid int){
	

}

//按行读取文件
func (wc *WorkerCli) ReadFile(reply *Msg_Reply,taskid int)  {
	for filePath,_ := range reply.Tasks[taskid].Files{
		f, err := os.Open("/Users/qiao/go/src/6.824/" +filePath)
		defer f.Close()
		if err != nil {
			panic(err)
		}
		buf := bufio.NewReader(f)
		for {
			line,err := buf.ReadBytes('\n')
			line = bytes.TrimSpace(line)
			wc.fileStream <- line
			if err != nil {
				if err == io.EOF{
					break
				}
				panic(err)
			}
		}
	}
	wc.isFinished <- true
	Info.Println("File Reading Finished!")
}

//发送心跳
func (wc *WorkerCli) StartHeart(){
	var retrytime int
	for{
		//只有在有Workerid的情况下才能开始心跳
		if wc.WorkID != InitWorkNum {
			if time.Now().UnixNano() - int64(wc.heartNexttime) > 0 {
				//如果 确认任务表 里有任务则 通过心跳发送
				args := NewHeart_Pack(wc.WorkID,wc.heartCount,wc.heartInterval)
				reply := NewHeart_Reply(wc.WorkID)
				wc.heartCount ++
				//如果消息确认列表里有待注册的消息那么就附加
				if len(wc.confirmTask) >0{
					Info.Printf("Found %d Confirm Msg Waiting to Send!",len(wc.confirmTask))
					args.LoadConfimTask(&wc.confirmTask)
				}
				//这里消费 已完成的任务
				if len(wc.workFinished) > 0 {
					for  {
						select{
							case respack:=<-wc.workFinished:
								//设置为 未提交状态
								t := wc.TaskTable[respack.TaskId]
								up := t.UpdataTaskStaus(TASKTYPE_COMMITED,true)
								if up == false{
									continue
								}
								//将 的任务的消息取出 发送给Master
								Info.Printf("Task %s Url %v  TaskType %d TaskStatus %d Message Will Send To Master!",respack.TaskId,respack.PathUrl,respack.TaskType,respack.Status)
								wc.TaskTable[respack.TaskId] = t
								args.TaskResult = append(args.TaskResult, respack)
						default:
							//消费完收工
							goto endselect
						}

					}
				}
				endselect:
				nexttime := time.Now().UnixNano()//记录发送时的时间
				Info.Printf("MyWorkId:%d Will Send  %d Times Hearts Waiting To ConfirmTaskId" +
					" Is :%v Have %d Task Finished !\n",args.WorkId,args.HeartId,args.ConfimTaskID,len(args.TaskResult))
				args.Sign()
				wc.call(RPC_HEARTMESSAGE,args,reply)
				if len(wc.alive) == 0{
					fmt.Println("【Warring】:chan to check woker online or not now is error has no value,please check error!")
					wc.alive <- false //如果特殊情况 chan 里没值 会触发 阻塞 这里做处理
				}
				if reply.Isccess{
					//将收到的消息 加入确认列表
					wc.isConfirm = append(wc.isConfirm, reply.AckConfirm...)
					//清除 确认任务表
					wc.confirmTask = wc.confirmTask[:0]
					wc.hearflg = true
					wc.heartNexttime = time.Duration(nexttime + int64(wc.heartInterval))//计算下一次发送的时间
					//如果 确认Master收到了这条消息
					if reply.AckFinished != nil{
						for _,v:= range reply.AckFinished{
							t := wc.TaskTable[v]
							up :=t.UpdataTaskStaus(TASKTYPE_END,true)
							if up == false{
								continue
							}
							wc.TaskTable[v] = t
						}
					}
					wc.setalive(true)
					time.Sleep(wc.heartInterval - time.Second - 1 )//间隔前1秒唤醒心跳 可调整
				}else{
					wc.setalive(false)
					//超时重新发送心跳
					if wc.tryouttime > 0 && retrytime <= wc.tryouttime {
						retrytime ++
					} else{
						//超时 并且重试过后还是没反应 就以后再试
						fmt.Println("【ERROR】Now Worker Is Offline,Will Sleep!")
						time.Sleep(wc.heartInterval)
						//初始化超时重试次数
						retrytime = 0
					}
				}
			}

		}else{
			fmt.Println("【ERROR】:Worker has not distribution work id noew!")
		}
	}
}

//从Master 收到心跳
func (wc *WorkInfo) HeartMonitor(){


}

func (wc  *WorkerCli) RegisterWorker(){
	//初始化 这个Worker的参数
	wi := NewWorkerInfo()
	wc.workdata = wi
	args := *NewMsg_Regs(wi, DEFAULTSTARGEGY)
	reply := NewReply_Regs()
	nexttime :=  time.Now().UnixNano()//记录发送时的时间
	fmt.Printf("[Info %s]  Will Regs  Work Info %s To Master \n",time.Now().Format("2006-01-02 15:04:05"),wi.ToString())
	args.Sign()
	wc.call(RPC_REGISTERWORK,args,reply)
	if reply.Verify() {
		// todo reply.SeverConfig 从服务端获取配置
		wc.heartNexttime = time.Duration(nexttime + int64(wc.heartInterval))//计算下一次发送的时间
		isAlived :=<- wc.alive
		state :="Offline"
		if isAlived == true{
			state ="Online"
		}
		fmt.Printf("Present Work State %s will Update to Online\n",state)
		wc.alive <- true//更新成在线
		wc.WorkID = reply.WorkID
	}else{
		panic(errors.New("Regs Work Fail!"))
	}
}

func (wc *WorkerCli ) isalive() bool{
	isalive :=<- wc.alive
	wc.alive <- isalive
	return isalive
}
func (wc *WorkerCli ) setalive(alive bool){
	state :="Offline"
	if wc.isalive() == false{
		if len(wc.alive)==0{
			wc.alive <- alive
			if alive == true{
				state ="Online"
			}
			Info.Printf("Present Work State %s will Update to Online",state)
		}else{
			tmp :=<- wc.alive
			if alive == true{
				tmp = tmp//无意义
				state ="Online"
			}
			Info.Printf("Present Work State %s will Update to Online",state)

			wc.alive <- alive
		}
	}else{
		Info.Println("Now Work State  is online Keeping....")
	}
}

func(wc *WorkerCli) CliStart() {
	//注册Worker
	wc.RegisterWorker()
	if wc.isalive() {
		//后台扫描 完成的任务
		go wc.selectFinished()
		//启动心跳
		go wc.StartHeart()
	if wc.workdata.Strategy == DEFAULTSTARGEGY {
		for{
			err := wc.Working()
			time.Sleep(time.Second * 1)
			if err!= nil{
				Error.Println(err)
			}
		}
	}
	}else{
		fmt.Println("work reg fail!")
	}
	select{}
}

func(wc *WorkerCli) call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//连接远程Rpc地址 调用函数
	//c, err := rpc.DialHTTP("UnixNano", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

//处理完成的任务
func (wc *WorkerCli) selectFinished() {
	for{
		//休眠2秒
		time.Sleep(time.Second * 2)
		if len(wc.workFinished) == 0{
			wc.workFinished = make(chan TaskPack,10)
		}
		//循环遍历任务 查找已经完成的任务
		for _,m := range wc.TaskTable {
			if m.Status == TASKTYPE_Finished && !m.Expired()  {
				tp := NewTaskPack()
				tp.SetPar(m.Partition)
				tp.SetStatus(FINISHED)
				tp.SetTaskId(m.TaskId)
				tp.SetTaskType(m.TaskTYpe)
				//对消息进行sha256
				tp.setSign()
				//把完成的任务丢进管道
				wc.workFinished <- *tp
			//没有完成 没有过期的
			}else if m.Status == TASKTYPE_PROCESS && !m.Expired(){
				Info.Printf(" Task %s Wating to finished Task !",m.TaskId)
			//没有完成  已经过期的
			}else if  m.Status == TASKTYPE_PROCESS && m.Expired(){
				Info.Printf(" Task %s is Expired!",m.TaskId)
			}

		}

	}

}

