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
	"hash/fnv"
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
	alive chan bool             //节点是否在线
	workdata *WorkInfo          //记录worker信息
	confirmTask []string        //如果Worker 收到了这个 任务就会添加到列表  通过心跳返回给Master 这样Master 就会确认你收到了
	rb *CacheBuf                //缓冲读取数据 排序 写出

}

func ihash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() & 0x7fffffff
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
	}
	a.alive <- false//初始默认节点处于离线状态
	//设置写出的处理函数
	a.rb.SetCacheBuffWriteFunc(DefaultWirteOutBuff)
	return a
}
//获取任务
func (wc *WorkerCli) Working(){
	for{
		isalive := wc.isalive()
		if isalive == false{
			fmt.Println("Work Now Is Offline Waiting To Register!")
			return
		}
		fmt.Printf("Worker Id %d Will Request Get Tak!",wc.WorkID)
		//初始化消息参数
		reply := NewMsg_Reply(wc.WorkID)
		args := NewMsg_Args(wc.WorkID, MapTask)
		//调用用获取任务
		args.Sign()
		wc.call(RPC_TASKDISTRIBUTION,args,reply)
		fmt.Print("【Info】Master Return Info:",reply.Message)
		//存在之前没完成的任务
		if reply.TaskType == UnfinishedTask {
			//todo 要么放弃 要么存储重新提交
			return
		}
		if !reply.Verify(){
			return
		}
		//将受到的确认 收到消息放回到列表 用于心跳时传回
		for _,m := range reply.Tasks{
			wc.confirmTask = append(wc.confirmTask, m.TaskId)
		}
		wc.confirmTask= append(wc.confirmTask, )
		//遍历收到的所有Task
		for i:=0;i< len(reply.Tasks);i++ {
			switch reply.Tasks[i].TaskTYpe {
			case MapTask:
				wc.doMapTask(args,reply,i)
			case ReduceTask:
				wc.doReduceTask(args,reply,i)
			default:
				fmt.Println("error!")
			}
		}

		//任务完成 传送到 文件系统 todo

		//同时master 任务已经完成 通过心跳



	}
}
func (wc *WorkerCli) doMapTask(args Msg_Args,reply *Msg_Reply,taskid int){
	//根据任务数初始化管道数目
	go wc.ReadFile(reply,taskid)
	fmt.Println("Prepare complete  task will run....")
		//携程处理文件读写
	go wc.CoreHandler()
}

func(wc *WorkerCli) CoreHandler(){
	fmt.Println("Start Read from chan")
	//初始化ringbuff
	for{
		select{
			case data := <- wc.fileStream:
				entry := wc.mapf("xx", string(data))
				for _,v := range entry{
					//写入环形缓冲区
					wc.rb.collect(v)
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
	fmt.Println("task finished!")
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
	fmt.Println("读文件锁!")
	wc.isFinished <- true
	fmt.Println("文件读取完毕!")
}

//发送心跳
func (wc *WorkerCli) StartHeart(){
	var retrytime int
	for{
		//只有在有Workerid的情况下才能开始心跳
		if wc.WorkID != InitWorkNum {
			if time.Now().UnixNano() - int64(wc.heartNexttime) > 0 {
				fmt.Println("【info】Send New Heart!")
				//如果 确认任务表 里有任务则 通过心跳发送
				args := NewHeart_Pack(wc.WorkID,wc.heartCount,wc.heartInterval)
				reply := NewHeart_Reply(wc.WorkID)
				//如果消息确认列表里有待注册的消息那么就附加
				if len(wc.confirmTask) >0{
					args.LoadConfimTask(&wc.confirmTask)
				}
				nexttime := time.Now().UnixNano()//记录发送时的时间
				fmt.Printf("【Info %s】 :Will Send Heart \n",time.Now().String())
				args.Sign()
				wc.call(RPC_HEARTMESSAGE,args,reply)
				if len(wc.alive) == 0{
					fmt.Println("【Warring】:chan to check woker online or not now is error has no value,please check error!")
					wc.alive <- false //如果特殊情况 chan 里没值 会触发 阻塞 这里做处理
				}
				if reply.Isccess{
					wc.heartNexttime = time.Duration(nexttime + int64(wc.heartInterval))//计算下一次发送的时间
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
			fmt.Printf("Present Work State %s will Update to Online",state)
		}else{
			tmp :=<- wc.alive
			if alive == true{
				tmp = tmp//无意义
				state ="Online"
			}
			fmt.Printf("Present Work State %s will Update to Online",state)

			wc.alive <- alive
		}
	}else{
		fmt.Println("Now Work State  is online Keeping....")
	}
}

func(wc *WorkerCli) CliStart() {
	//注册Worker
	wc.RegisterWorker()
	if wc.isalive() {
		//启动心跳
		go wc.StartHeart()
	if wc.workdata.Strategy == DEFAULTSTARGEGY {
		go wc.Working()
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

