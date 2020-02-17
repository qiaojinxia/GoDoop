# MapReduce
在学习MIt6.824 的过程中 想试着仿照Hadoop的原理实现MApReduce。现在已进把Shuffle部分的唤醒换冲去算法完成了。
先前已进写好了通过RPC 服务端将 N个文件分割成 按指定大小 分割成N个任务 每个人物 可以分配1个Worker 同时会给这个任务分配时间如果超时了
就会重新扔会管道中重新分发出去。

Map 流程: 数据 通过逐行读取 到 缓冲器 到达一定 容量后 多线程排序写出 最后将多个文件归并排序写出。
原理仿照了 hadoop 的mapreduce 。



![image-20200215194959698](/Users/qiao/go/src/godoop/src/image-20200215194959698.png)

# godoop# godoop
# godoop
# gopig
