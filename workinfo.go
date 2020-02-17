package src

import (
	"fmt"
	"time"
)

//服务端的一些信息
type Config struct {

}


//用来记录Worker 节点的一些信息
// 用于Master节点的管理
type WorkInfo struct {
	//work的名字
	Ip string
	//端口号
	Port int
	//主机cpu
	CpuInfo string
	//todo 采用主动式获取任务调用接口获取任务 或者被动式分配任务  0 1
	Strategy int
	//节点状态
	WorkState int
	//主机内存大小
	Memory uint64
	//记录每次通信之间的延迟
	Delay []uint64
	//记录心跳次数
	HeartNum uint64
	//定义多长时间Work 超时
	Outtime time.Duration
	//Worker拥有的任务
	Task map[string]Task
	//上次在线时间
	TimeStamp time.Duration
}

//客户端的一些信息
func NewWorkerInfo() *WorkInfo{
	return &WorkInfo{
		Ip:        "0.0.0.0",
		Port:      8999,
		CpuInfo:   "xxx",
		Memory:    0,
		Strategy:  DEFAULTSTARGEGY,
		WorkState: 1,
		HeartNum:  0,
		Delay:     nil,
		Task:      make(map[string]Task,0),
		Outtime:   time.Minute * WORKOUTTIME,
		TimeStamp: time.Duration(time.Now().UnixNano()),
	}
}
//当前主机是否
func (wi *WorkInfo) isExpire() bool{
	//如果当前时间大于 上一次 加上超时时间那么就算超时
	if time.Now().UnixNano() > int64(wi.Outtime +wi.TimeStamp){
		return true
	}
	return false
}
//超过最大过期时间
func (wi *WorkInfo) isExpireMax() bool{
	//如果当前时间大于 上一次 加上超时时间那么就算超时
	if time.Now().UnixNano() > int64(MAXWORKOUTTIME+wi.TimeStamp){
		return true
	}
	return false
}

func (wi *WorkInfo) ToString() string{
	res := fmt.Sprintf("Work IP:%s Port:%d, MachineInfo:%s , Strategy:%d,WorkState %d, Memory %d ,HeartNum %d ,TimeStamp %s",
		wi.Ip,wi.Port,wi.CpuInfo,wi.Strategy,wi.WorkState,wi.Memory,wi.HeartNum, formatTime(wi.TimeStamp))
	return res
}