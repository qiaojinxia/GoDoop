package src

import (
	"fmt"
	"time"
)

//排序缓存策略 如果 排序的两个key大于多少大小 对结果缓存
const SORTCACHE = 3

const  SPILLCAPACITY = 20.0
type TaskType int //任务类型 0 Map任务 1 Reduce任务
type KeyValue struct {
	Key   string
	Value string
}
func  (kv KeyValue) ToString() string{
	return fmt.Sprintf("%s:%s,",kv.Key,kv.Value)

}
const (
	MapTask              = 0               //表示map任务
	ReduceTask           = 1                //表示Reduce任务
	NoneTask			 = -1
	UnfinishedTask		= 2					//这个状态表明Worker 之前存在任务完成 就再次请求了
	InitWorkNum          = 0               //默认初始 WorkerId 表示未分配WorkId

	RPC_TASKDISTRIBUTION ="Master.GetTask" //获取任务函数
	RPC_HEARTMESSAGE    = "Master.HeartMonitor" //发送心跳方法
	RPC_REGISTERWORK 	= "Master.RegisterWorker" //注册工人方法

	DEFAULTSTARGEGY = 0		//默认策略  0 为主动请求模式 1为被动分配模式 通过 平衡hash算法 分配到Worker节点 todo

	WORKOUTTIME = 1 //Work超时时间 分钟
	TASKTIMEOUT = 60 //TASK超时时间 秒


	//todo
	//WORKER任务分配 智能分配 只有一个节点分配法
	MAXONEWORKER		= - 1	//按指定数量Task分配给一个Workers
	OWNMAX 				= 2 //OWNMAX 为0 则 霸道模式 管道里有几个全都要 直到管道里没有了


	AVGDISTRIBUTION		= 0		//平均分配 默认
	AUTODISTRIBUTION     = 1	//通过计算节点 算力智能分配
	MASTERDISTRIBUTIONMODEL = MAXONEWORKER //选项  MAXONEWORKER 指定获得数量 AVGDISTRIBUTION 每次一个先来先到  AUTODISTRIBUTION 根据算力只能分配task数量
	//----------------------------
	CANDEBETGETTASK = 0	//如果 节点还没完成之前的任务 又要重新发送请求 那么 允许 0 不允许


	CONFIRMTASKOUTTIME = 2 //秒 确认收到任务 如果master没在指定时间内收到消息 那么这个任务马上就会重新分配 超时时间 设定 要比 心跳时间短

	MSG_TIMEOUT  =time.Second * 60 //1分钟后消息超时


	MASTERSCAN = 2 //秒 扫描有没有过期的 任务或者Worker
	MAXWORKOUTTIME = time.Minute * 5 //分钟 超过最大过期时间 Master直接移除这个节点

)


