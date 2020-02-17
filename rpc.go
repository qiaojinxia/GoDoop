package src

import (
	"fmt"
	"time"
)

//
// RPC definitions.
//
//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Msg_Args struct {
	WorkNum uint64 //发送给Master 用来识别你身份的id
	TaskId string //如果任务完成 给Master发送 任务id
	TaskType int
	IsValid bool
	TimeStamp time.Duration //消息发送时间戳 消息发送超时丢弃
}
func (ma *Msg_Args) isExpire() bool{
	if ma.TimeStamp == 0 {
		return false
	}
	//如果 当前 时间 大于过期时间 则 过期
	return time.Now().UnixNano() > int64(ma.TimeStamp+MSG_TIMEOUT)
}

func (ma *Msg_Args) Veify() bool{
	if ma.isExpire(){
		fmt.Printf("Receiver a Expire Msg From Work ID: %d\n",ma.WorkNum)
		return false
	}
	if ma.IsValid == false{
		fmt.Printf("Receiver a Invaild Msg From Work ID: %d \n",ma.WorkNum)
		return false
	}
	return true
}
//初始化 一条请求
func NewMsg_Args(id uint64,tasktype int) Msg_Args{
	return Msg_Args{
		WorkNum: id,
		TaskId:  "",
		TaskType:tasktype,
		IsValid :false,
		TimeStamp:time.Duration(time.Now().UnixNano()),
	}
}
//对要发送的数据进行处理
func (ma *Msg_Args) Sign() {
	ma.TimeStamp = time.Duration(time.Now().UnixNano())
	ma.IsValid = true
}


//------------------------------


type Msg_Reply struct {
	Tasks    []Task         //返回给定的任务表
	WorkNum  uint64         //如果 第一次 连接Master 那么需要Master给你分配 一个 WorkId
	IsValid  bool           //标记当前消息 是否有效
	Message string          //返回的消息
	TaskType int            //返回的Task类型
	TimeStamp time.Duration //消息发送时间戳 消息发送超时丢弃
}


//判断消息是否有效
func (mp *Msg_Reply) Verify() bool {
	//如果没有被分配Workid 或者消息标志为false
	if !mp.IsValid {
		fmt.Println("the Message tag is invalid!")
		return false
	}
	if mp.WorkNum == InitWorkNum {
		fmt.Println("the Message receiver has no Workerid Distribution!")
		return false
	}
	//遍历任务列表 如果过期了 扔掉过期的任务
	newTask := make([]Task,0)
	for _,m:= range mp.Tasks{
		if !m.Expired() {
			newTask = append(newTask,m)
		}
	}

	//如果所有任务都过期了那么验证不通过
	if len(newTask) == 0{
		fmt.Println("Receiver All Tasks Had Expired!")
		return false
	}
	//将有效任务返回给 Worker 定义:没过期的算有效任务
	mp.Tasks = newTask
	if mp.IsValid == false {
		fmt.Println("the Message receive has Expired!")
		return false
	}
	return true
}


func (mp *Msg_Reply) isExpire() bool{
	if mp.TimeStamp == 0 {
		return false
	}
	//如果 当前 时间 大于过期时间 则 过期
	return time.Now().UnixNano() > int64(mp.TimeStamp+MSG_TIMEOUT)
}

//初始化 存放返回消息
func  NewMsg_Reply(id uint64) *Msg_Reply{
	return &Msg_Reply{
		Tasks:   make([]Task,0),
		WorkNum: id,
		IsValid: false,
		TimeStamp:time.Duration(time.Now().UnixNano()),
	}
}
//对要发送的数据进行处理
func (mp *Msg_Reply) Sign() {
	mp.TimeStamp = time.Duration(time.Now().UnixNano())
	mp.IsValid = true
}


//对要发送的数据进行处理
func (mp *Msg_Reply) Invaild() {
	mp.TimeStamp = time.Duration(time.Now().UnixNano())
	mp.IsValid = false
	mp.TaskType = NoneTask
}
//------------------------------------------------------
//注册消息的格式
type Msg_Regs struct {
	//要注册的work的信息 包含一些配置信息
	WorkData *WorkInfo
	WorkID uint64
	IsValid bool
	TimeStamp time.Duration //消息发送时间戳 消息发送超时丢弃
}

func NewMsg_Regs(info *WorkInfo,strategy int) *Msg_Regs{
	return &Msg_Regs{
		WorkData:  info,
		WorkID:    InitWorkNum,
		IsValid:   false,
		TimeStamp: time.Duration(time.Now().UnixNano()),
	}
}
func (mr *Msg_Regs) isExpire() bool{
	if mr.TimeStamp == 0 {
		return false
	}
	//如果 当前 时间 大于过期时间 则 过期
	return time.Now().UnixNano() > int64(mr.TimeStamp+MSG_TIMEOUT)
}

//对要发送的数据进行处理
func (mr *Msg_Regs) Sign() {
	mr.TimeStamp = time.Duration(time.Now().UnixNano())
	mr.IsValid = true
}
func (mr *Msg_Regs) Verify() bool{
	if mr.isExpire(){
		fmt.Printf("Waning: Get a Expire Message,Please Check ! %v",mr)
		return false
	}else if (!mr.IsValid){
		fmt.Printf("Waning: Get a Invaild Message,Please Check ! %v",mr)
		return false
	}
	return true
}

//-------------------------------------------------
//注册消息的格式
type Reply_Regs struct {
	IsSuccess bool
	//todo 服务端配置给 客户端的一些参数
	SeverConfig Config
	WorkID      uint64//工作id
	IsValid     bool
	TimeStamp   time.Duration //消息发送时间戳 消息发送超时丢弃
}
func (rr *Reply_Regs) Verify() bool{
	if rr.isExpire(){
		fmt.Printf("Waning: Get a Expire Message,Please Check ! %v",rr)
		return false
	}else if (!rr.IsValid){
		fmt.Printf("Waning: Get a Invaild Message,Please Check ! %v",rr)
		return false
	}
	if rr.IsSuccess {
		return rr.IsSuccess
	}
	return false
}
func (rr *Reply_Regs) isExpire() bool{
	if rr.TimeStamp == 0 {
		return false
	}
	//如果 当前 时间 大于过期时间 则 过期
	return time.Now().UnixNano() > int64(rr.TimeStamp+MSG_TIMEOUT)
}
func NewReply_Regs() *Reply_Regs{
	return &Reply_Regs{
		IsSuccess:   false,
		WorkID:      InitWorkNum,
		SeverConfig: Config{},
		IsValid:     false,
		TimeStamp:   time.Duration(time.Now().UnixNano()),
	}
}

//对要发送的数据进行处理
func (rr *Reply_Regs) Sign() {
	rr.TimeStamp = time.Duration(time.Now().UnixNano())
	rr.IsValid = true
	rr.IsSuccess =true
}

//-----------------------------------

//定义注册 心跳包
type Msg_Heart struct {
	HeartId uint64 //心跳id
	WorkId uint64 //工人id
	Interval time.Duration //心跳间隔
	IsValid bool		//消息是否有效
	ConfimTaskID []string //确认收到消息
	TimeStamp time.Duration //消息发送时间戳 消息发送超时丢弃
}
func (hp *Msg_Heart) isExpire() bool{
	if hp.TimeStamp == 0 {
		return false
	}
	//如果 当前 时间 大于过期时间 则 过期
	return time.Now().UnixNano() > int64(hp.TimeStamp+MSG_TIMEOUT)
}

//定义心跳 注册 包 初始化
func NewHeart_Pack(id uint64,heartid uint64,interval time.Duration) Msg_Heart {
	return Msg_Heart{
		HeartId:  heartid,
		WorkId:   id,
		IsValid:false,
		Interval: interval,
		ConfimTaskID:make([]string,0),
		TimeStamp:time.Duration(time.Now().UnixNano()),
	}
}
//向Master 确认 任务已收到
func (mh *Msg_Heart) LoadConfimTask(tasks *[]string){
	mh.ConfimTaskID = *tasks
}

//对要发送的数据进行处理
func (mh *Msg_Heart) Sign() {
	mh.TimeStamp = time.Duration(time.Now().UnixNano())
	mh.IsValid = true
}

//-------------------------------------------

//心跳信息
type Reply_Heart struct {
	WorkId uint64 //工人id
	Isccess bool
	IsValid bool
	TimeStamp time.Duration //消息发送时间戳 消息发送超时丢弃
}
//初始化 存放返回消息
func NewHeart_Reply(id uint64) *Reply_Heart {
	return &Reply_Heart{
		WorkId:  id,
		IsValid:false,
		TimeStamp:time.Duration(time.Now().UnixNano()),
	}
}


func (hr *Reply_Heart) isExpire() bool{
	if hr.TimeStamp == 0 {
		return false
	}
	//如果 当前 时间 大于过期时间 则 过期
	return time.Now().UnixNano() > int64(hr.TimeStamp+MSG_TIMEOUT)
}

//判断 心跳 或注册是否成功
func (hr *Reply_Heart) isHearSucess() bool{
	if hr.WorkId != InitWorkNum && !hr.isExpire() {
		return hr.Isccess
	}
	return false
}
//对要发送的数据进行处理
func (hr *Reply_Heart) Sign() {
	hr.Isccess = true
	hr.TimeStamp = time.Duration(time.Now().UnixNano())
	hr.IsValid = true
}