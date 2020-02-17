package TEST
//
//import (
//	"6.824/src/mr"
//	"bufio"
//	"encoding/json"
//	"fmt"
//	"io"
//	"os"
//	"sort"
//)
//import "log"
//import "net/rpc"
//
////
//// Map functions return a slice of KeyValue.
////
//type KeyValue struct {
//	Key   string
//	Value string
//}
//func  (kv KeyValue) ToString() string{ return fmt.Sprintf("%s%s",kv.Key,kv.Value)}
//
////order
//// for sorting by key.
//type ByKey []KeyValue
//
//// for sorting by key.
//func (a ByKey) Len() int           { return len(a) }
//func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
//func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
////type worker struct {
////	//收到的员工编号
////	wokenum  int
////}
//////
//// use ihash(key) % NReduce to choose the reduce
//// Tasks number for each KeyValue emitted by Map.
////
//
//func Worker(mapf func(string, string) []KeyValue,
//	reducef func(string, []string) string) {
//	//初始化消息参数
//	args := mr.Msg_Args{}
//	args.WorkNum = mr.InitWorkNum
//	// declare a reply structure.
//	reply := mr.Msg_Reply{}
//	reply.WorkNum = mr.InitWorkNum
//	//循环获取任务
//		// send the RPC request, wait for the reply.
//		call("Master.GetTask", args, &reply)
//		//判断消息是否有效
//		if reply.IsValid == false{
//			panic("Invaild Data From Maste !")
//		}
//		fmt.Printf("Running.... Worker Num : %d \n",reply.WorkNum)
//		fmt.Println("xxxx",reply.Tasks.Partition.Url)
//	switch reply.Tasks.TaskTYpe {
//		case mr.MapTask:
//			for _,v := range reply.Tasks.Files{
//				//读取文件
//				conetnt,err := ReadFilex("../main/" + v,1024)
//				if err != nil{
//					//如果读取失败
//					fmt.Println("Read File err!",err)
//					panic(conetnt)
//				}
//				fmt.Printf("Hanlder Map File Name : %s  \n",v)
//				kvtmp := mapf(v, string(conetnt))
//				fmt.Printf("Map File %s Finish ! \n  ",v)
//
//				//对kv进行排序
//				sort.Sort(ByKey(kvtmp))
//				fmt.Println("Sort Finished!")
//				//进行shuffle排序
//				for _,list := range kvtmp  {
//					num := mr.ihash(list.Key) % reply.Tasks.Partition.NReduce
//					fmt.Println("par num",num)
//					//取出引用指针
//					keys := reply.Tasks.Partition.Url[num]
//					//如果没有被创建
//					if keys == nil {
//						fmt.Println("NO Create~")
//						keys = make([]string,0)
//					}
//					contains := false
//					for _, m:= range keys{
//						//如果包含
//						if m == list.Key{
//							contains = true
//						}
//					}
//					//如果不包含
//					if contains == false{
//						fmt.Println("apeend kv")
//						//取得数组
//						keys = append(keys, list.Key)
//						//更新
//						reply.Tasks.Partition.Url[num] = keys
//					}
//					jsons, errs := json.Marshal(list) //转换成JSON返回的是byte[]
//					if errs != nil {
//						fmt.Println("Serialize To Json Err!",errs)
//						panic(errs)
//					}
//					filename :=  fmt.Sprintf("../main/" + "map-out-%d",num)
//					fmt.Println("append to filename",filename)
//					appendToFile(filename,string(jsons))
//				}
//			}
//		case mr.ReduceTask:
//			return
//
//
//	}
//
//
//		////如果你没得到过工号 并但是给你返回了工号那么你就记录下这个工号
//		//if args.WorkNum == InitWorkNum && reply.WorkNum != InitWorkNum{
//		//	args.WorkNum =reply.WorkNum
//		//	fmt.Printf("Get Work Nums %d !",reply.WorkNum)
//		//	//args 记录了 这个works 的工号 如果返回个参数里 没给你指定 工号 并且 你自己没有得到过工号就不能工作
//		//}else if args.WorkNum == InitWorkNum {
//		//	//如果没有工作号 那么就不能就行工作
//		//	fmt.Println(args.WorkNum,reply.WorkNum)
//		//	panic(errors.New("Work Num Cannot None Cannot Work!"))
//		//}
//		//if reply.TaskType == MapTask{
//		//	fmt.Printf("Reply Map Files: %s  My Worker Num : %d \n", reply.FileList,reply.WorkNum)
//		//	for _, fl := range reply.FileList{
//		//		//读取被分配的本地文件 进行Map
//		//		conetnt,err := ReadFilex("../main/" + fl,1024)
//		//		if err != nil{
//		//			fmt.Println("Read File err!",err)
//		//			return
//		//		}
//		//		fmt.Printf("Hanlder Map File Name : %s  \n",fl)
//		//		kv := mapf(fl, string(conetnt))
//		//		fmt.Printf("Map File %s Finish ! \n  ",fl)
//		//		jsons, errs := json.Marshal(kv) //转换成JSON返回的是byte[]
//		//		if errs != nil {
//		//			fmt.Println("Serialize To Json Err!",errs)
//		//			return
//		//		}
//		//		//保存的文件名
//		//		filename := "../main/" + "map-out-" + fl
//		//		WriteFile(filename, string(jsons))
//		//		fmt.Printf("Write File %s To Disk  Finished!\n",fl)
//		//		//成功后 把完成的加入列表 以便 提交给Master 进行记录
//		//		args.DoneFileList[fl] =  filename
//		//	}
//		//	fmt.Println("Tasks Done Notify To Master!" ,args.DoneFileList)
//		//	//调用Rpc 告诉MAster 我的任务完成了 并把保存的文件名告诉 Master
//		//	error := call("Master.WorkeDoneInvock",&args, &reply)
//		//	if (error == false ){
//		//		fmt.Println("Notify To Master Fail!",error)
//		//		return
//		//	}
//		//
//		//} else if reply.TaskType == ReduceTask {
//		//	fmt.Printf("Get Reduce Tasks To Process !",reply.FileList)
//		//	var intermediate []KeyValue
//		//	for _, fl := range reply.FileList{
//		//		fmt.Println("reduce file"+ fl)
//		//		//从磁盘Reduce 读取文件
//		//		conetnt,err := ReadFilex("../main/" +fl,1024)
//		//		var contents []KeyValue
//		//		if err != nil{
//		//			fmt.Printf("Read Erduce File %s err!", fl,err)
//		//			return
//		//		}
//		//		error := json.Unmarshal(bytes.Trim(conetnt,"\x00"),&contents)
//		//		if error != nil{
//		//			fmt.Printf("Serialize ReduceFile %s To Json Fail  err!", fl,err)
//		//			return
//		//		}
//		//		//将多个文件的键值对添加进内存
//		//		intermediate = append(intermediate,contents...)
//		//	}
//		//	//shuffle 排序
//		//	sort.Sort(ByKey(intermediate))
//		//	//输出文件
//		//	oname := "mr-out-0"
//		//	ofile, _ := os.Create(oname)
//		//	i := 0
//		//	for i < len(intermediate) {
//		//		j := i + 1
//		//		//如果是相同的key 则 继续遍历 直到 遇到不相同的key
//		//		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
//		//			j++
//		//		}
//		//		values := []string{}
//		//		for k := i; k < j; k++ {
//		//			values = append(values, intermediate[k].Value)
//		//		}
//		//		output := reducef(intermediate[i].Key, values)
//		//
//		//		// this is the correct format for each line of Reduce output.
//		//		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
//		//
//		//		i = j
//		//	}
//		//
//		//	ofile.Close()
//		//
//		//}
//		////休眠1秒
//		//time.Sleep(time.Second * 1)
//		//fmt.Printf("worker %d finish!",counter)
//		//counter ++
//
//	//handler map Tasks and savefile to local disk and finally senddonde to master
//
//
//
//}
//
//func appendToFile(file, str string) {
//	f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0660)
//	if err != nil {
//		fmt.Printf("Cannot open file %s!\n", file)
//		return
//	}
//	defer f.Close()
//	f.WriteString(str)
//}
//
//
////
//// example function to show how to make an RPC call to the master.
////
//func CallExample() {
//
//	// declare an argument structure.
//	args := mr.Msg_Args{}
//
//	// declare a reply structure.
//	reply := mr.Msg_Reply{}
//
//	// send the RPC request, wait for the reply.
//	call("Master.getTask", &args, &reply)
//
//	// reply.Y should be 100.
//	//fmt.Printf("reply.Num %v reply filenames  %v \n",reply.WorkNum, reply.FileList)
//}
//
////
//// send an RPC request to the master, wait for the response.
//// usually returns true.
//// returns false if something goes wrong.
////
//func call(rpcname string, args interface{}, reply interface{}) bool {
//	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
//	c, err := rpc.DialHTTP("UnixNano", "mr-socket")
//	if err != nil {
//		log.Fatal("dialing:", err)
//	}
//	defer c.Close()
//	err = c.Call(rpcname, args, reply)
//	if err == nil {
//		return true
//	}
//
//	fmt.Println(err)
//	return false
//}
//
//func ReadFilex(filePth string, bufSize int) ([]byte,error) {
//	content := make([]byte, 1024) //一次读取多少个字节
//	f, err := os.Open(filePth)
//	if err != nil {
//		return nil,err
//	}
//	defer f.Close()
//	buf := make([]byte, bufSize) //一次读取多少个字节
//	bfRd := bufio.NewReader(f)
//	for {
//		n, err := bfRd.Read(buf)
//		if err != nil { //遇到任何错误立即返回，并忽略 EOF 错误信息
//			if err == io.EOF {
//				return content,nil
//			}
//			return nil,err
//		}
//		for m := range buf[:n]{
//			content = append(content,buf[m] )
//		}
//
//	}
//
//	return nil,nil
//}
//func WriteFile(filePth string,conetnt string){
//	f, err := os.Create(filePth)
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//	l, err := f.WriteString(conetnt)
//	if err != nil {
//		fmt.Println(err)
//		f.Close()
//		return
//	}
//	fmt.Println(l, "bytes written successfully")
//	err = f.Close()
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//}