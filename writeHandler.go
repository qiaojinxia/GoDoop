package src

//对溢出文件进行归并处理
import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const OUTPUTFILENAME = MAPDIR + "map-spill-out-%s-%d"


type outInfo struct {
	offset uint32 //文件内偏移
	ptr uint32 //记录指针位置
	flagse map[string][]byte //8个字节 保存开始到结束的位置每4个字节
	buff *[]byte
}
/*
	reduce
*/
type kvContent struct {
	sector []*outInfo
	url    string //文件路径
}

func NewOutInfo() *outInfo{
	oi := &outInfo{
		offset:0,
		ptr:0,
		flagse: make(map[string][]byte,0),
		buff:nil,
	}
	a := make([]byte,0,0)
	oi.buff = &a
	return oi
}

/*
传入 键 和 键值的长度 记录在文件中的位置
*/
func (oi *outInfo) addOutInfo(value *string,k uint32) error{
	buff := oi.buff
	kv := strings.Split(*value,":")
	lens := uint32(len(*value))
	key := kv[0]
	var err error
	if oi.getOutInfo(key) ==nil {
		arr := make([]byte,0)
		arr = append(arr, IntToBytesU32(oi.ptr)...)
		arr = append(arr, IntToBytesU32(oi.ptr +lens-1)...)
		err = oi.setOutInfo(key,arr)

	}else{
		arr := make([]byte,0)
		start :=oi.flagse[key][:4]
		arr = append(arr, start...)
		arr = append(arr, IntToBytesU32(oi.ptr +lens-1)...)
		err = oi.setOutInfo(key,arr)
	}
	oi.ptr = oi.ptr + lens
	*buff = append(*buff, *value...)
	return err
}

func (oi *outInfo) getBuff() *[]byte{
	return  oi.buff
}

func (oi *outInfo) cleanBuff() {
	oi.buff = nil
}

func (oi *outInfo) setOutInfo(key string,indexpoint []byte) error{
	if len(indexpoint) == 8{
		oi.flagse[key] = indexpoint
		return nil
	}
	return errors.New("flage size error!")
}


func (oi *outInfo) getOutInfo(key string) []uint32{
	res := make([]uint32,2)
	if oi.flagse[key] !=nil {
		begin:= BytesToUint32(oi.flagse[key][:4])
		end:= BytesToUint32(oi.flagse[key][4:])
		res[0] = begin
		res[1] = end
		return res
	}
	return nil
}


func NewKvContent(nreduce uint32) *kvContent{
	kv := &kvContent{
		sector: make([]*outInfo,nreduce),
	}
	return kv
}


//key string,index uint32,len uint32
//传入 k : 分区 value : kv值  格式: XXX:XXX,
func (kvc *kvContent) addKVContent(k uint32,value *string){
	//查找 k分区
	a:= kvc.sector[k]
	//如果不存在 new一个
	if a == nil {
		a = NewOutInfo()
		kvc.sector[k] = a
	}
	err := a.addOutInfo(value,k)

	if err != nil{
		panic(err)
	}
}

//输出数据
type storeHandler struct {
	lock sync.Mutex
	wg sync.WaitGroup
	nReduce uint32
	num     uint32; //写出次序
	fileout map[string]*kvContent
	keys 	[]*[]string
	url 	[]string //保存文件的目录
	filename string
	taskname  string
	successwrite bool //成功写出
}
func NewStoreHandler(nReduce uint32,taskname string) *storeHandler {
	return &storeHandler{
		nReduce: nReduce,
		num:     0,
		fileout: make(map[string]*kvContent, 0),
		keys:    make([]*[]string, nReduce,nReduce),
		filename:fmt.Sprintf(OUTPUTFILENAME,taskname,0),
		taskname:taskname,
		successwrite:false,
	}
}
func (sh *storeHandler)	isExists(par uint32,key string) bool{
	keylist := sh.keys[par]
	for i:=0;i<len(*keylist);i++{
		ix := uint32(i)
		if (*keylist)[ix] == key{
			return true
		}
	}
	return false
}

func (sh *storeHandler) Handler(par uint32,kv *string){
	//begin:= time.Now()
	sh.lock.Lock()
	var a []string
	kvarr := strings.Split(*kv,":")
	if sh.keys[par] == nil {
		a = make([]string,0)
		sh.keys[par] = &a
	}else{
		a = *sh.keys[par]
	}
	var kvc *kvContent
	if sh.fileout[sh.filename] == nil{
		kvc = NewKvContent(sh.nReduce)
		sh.fileout[sh.filename] = kvc
	}else{
		kvc = sh.fileout[sh.filename]
	}
	exists := sh.isExists(par,kvarr[0])
	if !exists{
		a = append(a, kvarr[0])
	}
	sh.keys[par] = &a
	sh.wg.Wait()
	//for _,v := range sh.keys{
	//	sh.wg.Add(1)
	//	go 	ShellSort(&sh.wg,v)
	//}
	sh.lock.Unlock()
	kvc.addKVContent(par,kv)
	//fmt.Println(time.Now().Sub(begin))

}

func (sh *storeHandler) WriteOutBuff(){
	bg := time.Now()
	kvc := sh.fileout[sh.filename]
	var ptr uint32
	for _,i:= range kvc.sector{
		buff := i.getBuff()
		if buff == nil{
			continue
		}
		//累加偏移
		appendToFile(sh.filename, string(*buff))
		i.offset = ptr
		ptr= uint32(len(*buff)) + ptr
		i.cleanBuff()
	}
	//文件的 路径
	sh.url= append(sh.url, sh.filename)
	Info.Printf("写出花费时间 %d 毫秒 ",time.Now().Sub(bg).Milliseconds())
	sh.num ++
	sh.filename = fmt.Sprintf(OUTPUTFILENAME,sh.taskname,sh.num)

}

//将多个溢出 排序后合并
func (sh *storeHandler) MergeFile(){
	sh.lock.Lock()
	tmpurl := make([]string,0)
	keyslen := 0
	for _,v := range sh.keys{
		keyslen += len(*v)
	}
	fmt.Println("keynums",keyslen)
	for i:=0;i < len(sh.keys);i++{
		pathx := fmt.Sprintf(MAPDIR + "map-out-%s-%d",sh.taskname,i)
		tmpurl = append(tmpurl, pathx)
		i2 := *sh.keys[uint32(i)]
		for m:=0;m<len(i2);m++{
			//count := 0
			for filepath,v := range sh.fileout{
				a := v.sector[uint32(i)]
				if a == nil{
					continue
				}
				ab := a.flagse[i2[m]]
				if len(ab) != 8  {
					continue
				}
				start := BytesToUint32(ab[:4])  + a.offset
				end := BytesToUint32(ab[4:]) + a.offset
				counter := 0
				for{
					content,err := read(start,end,filepath,counter)
					if err != nil && content == nil{
						break
					}
					appendToFile(pathx, string(content))
					counter ++
					content= nil
					if err != nil {
						break
					}

				}
				//count ++

			}
			//if count != 1{
				//panic("error")
			//}
		}
	}
	for _,v := range sh.url{
		err := os.Remove(v)
		if err != nil {
			fmt.Println("can't find path error!")
		}
	}
	url := make([]string,0)
	for _,p := range tmpurl{
		url = append(url,p)
	}
	sh.url = url
	//成功写出
	sh.successwrite = true
	sh.lock.Unlock()
}