package src
/* @Create by:caomaoboy
// 2019-01-07
*/
import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

const (
	NMETA int = 4;//元数据一个字段占用字节 byte
	METASIZE = NMETA * 4; //一整个元数据占用长度
	HANDLER = 0
	WRITEOUTBUFF = 1
	MERGEFILE = 2
)
func ReadWithSelect(ch chan bool) (x int, err error) {
	select {
	case  <-ch:
		return x, nil
	default:
		return 0, errors.New("channel has no data")
	}
}
type metadata []byte
func (mb metadata) getkey(kvbuffer *[]byte) []byte{
	d := mb.BytesToInt()
	part1 := make([]byte,0,12)
	if d[0]< d[1]+1{
		return (*kvbuffer)[d[0]:d[1]+1]
	}
	part1 = append(part1,(*kvbuffer)[d[0]:]...)
	part1 = append(part1, (*kvbuffer)[0:d[1]+1]...)
	return part1
}

func (mb metadata) compar(cache map[string]map[string]int,kvbuffer *[]byte,str1 []byte) int {
	d:=mb.getkey(kvbuffer)
	st1 := string(d)
	st2 := string(str1)
	//大于3个字符开启缓存
	if len(st1) >=SORTCACHE && len(st2) >= SORTCACHE{
		m1 ,ok := cache[st1]
		if ok {
			m2 ,ok1 := m1[st2]
			if ok1{
				//Info.Printf("命中缓存 k1:%s (%d) k2:%s",st1,m2,st2)
				return m2
			}else{
				goto bk
			}
		}else{
			goto bk
		}
	}
	bk:
	res := CompareString(&st1,&st2)
	//缓存结果
	if len(d) >=SORTCACHE && len(str1) >= SORTCACHE{
		m2 := make(map[string]int)
		m2 [string(str1)] = res
		cache[string(d)] = m2
	}
	return res

}

func (mb metadata) getKV(kvbuffer *[]byte) ([]byte,uint32,int){
	d := mb.BytesToInt()
	//fmt.Println("keystart",d[0],"keyend",d[1]+d[3]+1)
	zjy :=  d[1] - d[0] + 1
	if d[0] > d[1]{
		zjy =  len(* kvbuffer)  - d[0] + d[1] + 1
	}
	qjx := zjy + d[3]
	if d[0] + qjx -1 >= len(* kvbuffer) {
		kvmap := make([]byte,0,qjx)
		loc := (d[0] + qjx -1) % len(* kvbuffer)
		kvmap = append(kvmap,(*kvbuffer)[d[0]:]...)
		kvmap = append(kvmap,(*kvbuffer)[0:loc+1]...)
		if len(kvmap) != qjx {
			Error.Println("Error:System has a Wrong!",len(kvmap),d)
			panic("Metadata size not math!")
		}
		return kvmap, uint32(d[2]),qjx
	}
	return (*kvbuffer)[d[0]:d[0] + qjx], uint32(d[2]),qjx
}

type MetaInt struct {
	kvbuffer *[]byte //指向buffer
	metastart *int //元数据开始
	metaend *int //元数据结尾
	metapoint []*metadata //记录指向每个metadata的指针
	metamark int//记录移动数据
	isexchange chan bool //扇区转换那一刻用于阻塞
	metachan *[]chan *metadata
}
func (md *MetaInt) inital(){
	count := md.getlen()
	p :=make([]*metadata,0)
	for i := 0 ;i<count; i++  {
		m,err := md.get(i)
		if err != nil{
			panic(err)
		}
		//记录指针
		p = append(p,&m)
	}
	md.metapoint = p
}

func (md *MetaInt) getlen() int{
	buflen :=len(*md.kvbuffer) -1
	// 判断有没有在环头 和环尾

	if *md.metastart < (*md.metaend % (buflen +1)) {
		return (*md.metastart  + 1 + buflen - *md.metaend  + 1)/ METASIZE
	}
	return (*md.metastart - *md.metaend  + 1 ) / METASIZE //计算环形区的数组长度
}
//从最后一个索引开始取元数据区信息
func (md *MetaInt) getInverse(num int)  (metadata,error){
	metalen := md.getlen() -1
	if num > metalen {
		return nil,errors.New("Out Of Bufferbyte")
	}
	metadata,err := md.get(metalen - num )
	return metadata,err

}
//给定索引取元数据区的内容 从 kvstart 开始取 每次 减去 16 个字节
func (md *MetaInt) get(num int) (metadata,error){
	res := make([]byte,0)
	var tmp []byte
	tmp = *md.kvbuffer
	//超出数组数组长度报错
	if num > md.getlen() -1{
		return nil,errors.New("Out Of Bufferbyte")
	}
	buflen := len(*md.kvbuffer) -1
	offeset := *md.metastart - (num + 1) * METASIZE //计算偏移 从零索引开始
	if offeset + 1 < 0{
		offeset += 1
		offeset = buflen - ((- offeset) % buflen) + 1
	}else {
		offeset+=1
		res = append(res,tmp[offeset:offeset+METASIZE]...)
		return res,nil
	}
	indexend :=  offeset + METASIZE -1 //如果索引 + 16 个字节 超出数组尾部 则 到环首处理
	//如果索引 是 1022 数组总长 1023 那么 1022:1023 会有 14 个剩余
	// 字节 在 0:13 处理 一半索引落在环尾部 一半落在 环首情况
	if indexend > buflen{
		partinde := buflen - offeset + 1
		res = append(res, tmp[offeset:buflen+1]...)
		res = append(res,tmp[0: METASIZE  - partinde ]...)
		return res,nil
	}
	return tmp[offeset:indexend+1], nil
}

func NewMetaInt(kvbuffer *[]byte,kvstart *int,kvend *int) *MetaInt{
	return &MetaInt{
		kvbuffer: kvbuffer,
		metastart:kvstart,
		metaend:kvend,
		metapoint:nil,
		metachan:nil,
	}
}

//创建元数据
func NewMetaData(keystart int, keyend int,kvpartition uint32, vallen int) metadata{
	md:= make([]byte,0)
	md = append(md,IntToBytes(keystart)...)
	md = append(md,IntToBytes(keyend)...)
	md = append(md, IntToBytesU32(kvpartition)...)
	md = append(md,IntToBytes(vallen)...)
	return md
}

//整形转换成字节
func IntToBytes(n int) []byte{
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf,uint32(n))
	return buf
}
//字节转换成整形
func(md metadata) BytesToInt() []int {
	x := make([]int,0,4)
	for i:=0;i<4;i++{
		x = append(x,int(BytesToUint32(md[:4])))
		md = md[4:]
	}
	return x
}
//对元数据区 格式化输出
func formmat(in []int){
	Info.Printf("keystart: %d valstart: %d , kvpartition: %d ,vallen: %d nextkeystart: %d \n",in[0],in[1]+1,in[2],in[3],in[1] + in[3] + 1)
}

type CacheBuf struct {
	chokesignal chan bool
	//当移动ringbuff数据时 需要加锁
	lock sync.Mutex
	//写出chan互斥锁
	lock2 sync.Mutex
	//写出时锁
	lock3 sync.Mutex
	//信号所
	lock4 sync.Mutex
	//引用 kvbuffer单独对元数据区进行索引 方便取出数据
	metaint *MetaInt
	//标记kv数据开始位置
	bufstart int;
	//下次要写入的kv数据的位置
	bufindex int
	//写出时 从kvstart 开始 每次加上写出的 kv长度 写出完了 和 bufend 应该是一致 用来校验写出的kv长度是否相符
	bufmark int
	//溢出时kv数据的结束位置
	bufend int
	//下次要插入的索引的位置
	kvindex int
	//溢出时索引的结束位置
	kvend int
	//溢出时索引的起始位置
	kvstart int
	//除法阻塞阙值
	overflowval    int
	//触发溢出值
	spillerprecent float64
	//处理缓冲溢出的函数
	RingBuffWriteHandler func(*storeHandler,int,uint32,*string)
	//分区数
	parnum uint32

	wg3 sync.WaitGroup
	wg2 sync.WaitGroup
	wg sync.WaitGroup
	//溢出id
	spillid uint64
	//处理缓冲溢出的文件
	sh *storeHandler
	//核心存储buff
	kvbuffer []byte

	spilloutdeadlock int
	tag chan bool
	strbuff * bytes.Buffer
	//写出文件次数
	spilloutime int

	//排序缓存
	sortcache [] map[string]map[string]int

}


func(rb *CacheBuf) CacheBuffSpillOut(funno int,k uint32,c *string){
	rb.RingBuffWriteHandler(rb.sh,funno,k,c)
}

func(rb *CacheBuf) SetCacheBuffWriteFunc(a func(*storeHandler,int,uint32,*string)){
	rb.RingBuffWriteHandler = a
}


func NewCacheBuf(nReduce uint32,size int) *CacheBuf {
	rb := &CacheBuf{
		chokesignal:          make(chan bool,nReduce),
		bufstart:             0,
		wg:                   sync.WaitGroup{},
		wg2:                  sync.WaitGroup{},
		bufindex:             0,
		bufend:               0, //buf缓冲区的
		kvindex:              size -16 ,
		bufmark:              0,
		kvend:                size -1 ,
		kvstart:              size -1,
		kvbuffer:             make([]byte,size,size),
		parnum:               nReduce,
		RingBuffWriteHandler: nil,
		spillid:              0,
		sh:                   NewStoreHandler(nReduce),
		spilloutdeadlock:     0,
		tag:                  make(chan bool,1),
	}

	rb.metaint = NewMetaInt(&rb.kvbuffer,&rb.kvstart,&rb.kvend)
	metd := make([]chan *metadata,rb.parnum)
	for i:=0;i<int(nReduce);i++{
		metd[i] = make(chan *metadata,1024)
	}
	rb.sortcache = make([]map[string]map[string]int,nReduce)
	for i:=0;i<int(nReduce);i++{
		sortcache := make(map[string]map[string]int,0)
		rb.sortcache[i] =sortcache
	}
	rb.tag <- false
	rb.strbuff = &bytes.Buffer{}
	rb.metaint.metachan = &metd
	rb.spilloutime =0
	return rb
}

//计算加上下一个值是否会溢出
func (rb *CacheBuf) preSurplusSpace(kvlen int) (float64,error){
	kval :=rb.kvstart - rb.kvend + METASIZE
	if kval < 0 {
		kval = len(rb.kvbuffer) - rb.kvend   + rb.kvstart + METASIZE

	}
	bval := rb.bufindex - rb.bufstart + kvlen

	if bval < 0 {
		bval = rb.bufindex  + len(rb.kvbuffer) - rb.bufstart + kvlen
	}
	val := len(rb.kvbuffer) - kval - bval
	//fmt.Printf("Residual byte:%d Usage ratio:%.0f %s \n",val,float64(val)/float64(len(rb.kvbuffer))* 100,"%")
	res:= float64(val)/float64(len(rb.kvbuffer))* 100
	if res < 0 {
		err := errors.New("capility overflow error!")
		return res,err
	}
	return res,nil

}
//写出缓存内所有剩余
func(rb *CacheBuf) SpillAll(){
	rb.lock3.Lock()
	if rb.spilloutdeadlock != 0  {
		panic("fonud deadlock!")
	}
	rb.spilloutdeadlock += 1
	rb.wg3.Wait()
	rb.metaint.metamark = rb.metaint.getlen()
	rb.metaint.inital()
	rb.wg3.Add(1)
	rb.spilloutdeadlock -= 1
	Info.Println("Spill All the content!")
	//var keylen int
	//metapoints := len(rb.metaint.metapoint)
	//对数据进行排序
	rb.wg2.Add(1)
	go rb.sortMetaData()
	rb.wg2.Wait()
	////开一个携程 进行检测 排序内容是否全部输出完
	go singlemonitor(&rb.wg,&rb.chokesignal, int(rb.parnum))
	for  i:=0;i < len(*rb.metaint.metachan);i++ {
		rb.wg2.Add(1)
		go rb.WriteOutChan(i)
	}
	rb.wg2.Wait()
	Info.Println("写出全部!")
	rb.RingBuffWriteHandler(rb.sh,WRITEOUTBUFF,0,nil)
	//
	rb.RingBuffWriteHandler(rb.sh,MERGEFILE,0,nil)
	rb.setTag(false)
	rb.ClearBuff()
	rb.wg3.Done()
	rb.lock3.Unlock()
}

func (rb *CacheBuf) ClearBuff (){
	//清空容器指针
	rb.bufindex = 0
	rb.bufmark =  0
	rb.bufstart = 0
	rb.bufend = 0
	rb.kvstart = len(rb.kvbuffer) - 1
	rb.kvend = len(rb.kvbuffer) - 1
	rb.kvindex = len(rb.kvbuffer) - METASIZE


}
func(rb *CacheBuf) setTag(tag bool){
	if len(rb.tag) > 0 {
		<- rb.tag
	}
	rb.tag <- tag
}
func(rb *CacheBuf) getTag() bool{
	if len(rb.tag) > 0 {
		tmp := <- rb.tag
		rb.tag <- tmp
		return tmp
	}
	return false
}

func (rb *CacheBuf) collect(kv KeyValue){
	rb.lock3.Lock()
	keyend := rb.bufindex+ len(kv.Key) -1 //键的结束地址
	vallen := len(kv.Value) + 2
	kvstr := kv.ToString()
	kvstrlen := len(kvstr)
	//如果kv的大小直接 超过ringbuff的容量直接 写出
	if kvstrlen + METASIZE > len(rb.kvbuffer){
		Info.Println("Waring: ByteBUff is small than kv length ,Will write directly")
		appendToFile("map-out-ov",kvstr)
		rb.spillid ++
	}


	space ,_ := rb.surplusSpace()
	//容量不足
	if space <= SPILLCAPACITY && rb.getTag() == false {
		rb.wg3.Wait()
		space ,_ := rb.surplusSpace()
		if space <= SPILLCAPACITY && rb.getTag() == false{
			rb.setTag(true)
			//读取bufstart 到bufend
			rb.bufend = rb.bufindex
			rb.bufmark = rb.bufstart
			//触发写出
			rb.metaint.metamark = rb.metaint.getlen()
			rb.metaint.inital()
			Info.Println("[Info]Start recycling with less than 20% of the remaining capacity")
			rb.wg.Wait()
			rb.wg2.Wait()
			rb.wg2.Add(1)
			go rb.SplliOut()
		}
	}
	//达到阈值 暂停这次写入ringbuff
	threshold,err:= rb.preSurplusSpace(kvstrlen)
	if threshold < 1.0 && err != nil{
		for rb.getTag()  == true{
			Info.Println("CacheBuff Is Full Waiting Gc...")
			time.Sleep(time.Millisecond * 300)
		}
	}else if err != nil{
		panic(err)
	}

	//移动移动指针时要加锁
	rb.lock.Lock()
	rb.bufindex = rb.bufindex % len(rb.kvbuffer)
	keyend = keyend % len(rb.kvbuffer)
	//记录要插入的kv的k开始索引
	tmpmark := rb.bufindex
	for i := 0 ;i <  kvstrlen;i++{
		rb.kvbuffer[rb.bufindex] = (kvstr)[i]
		rb.bufindex++
		if rb.bufindex > len(rb.kvbuffer) - 1 {
			rb.bufindex = rb.bufindex % len(rb.kvbuffer)
		}
	}
	par := ihash(kv.Key) % rb.parnum
	//传入 buf开始地址 val长度
	rb.addmetadata(tmpmark ,keyend,vallen,par)
	//bufindex 指向 下一次写入的索引
	rb.lock.Unlock()
	rb.lock3.Unlock()
}
//写出数据
func (rb *CacheBuf) WriteOutChan(threadi int) {
	//defer Info.Printf("[DEBUG]线程 %d 退出!\n",threadi)
	defer rb.wg2.Done()//等价于 wg.Add(-1)
	cd := (*rb.metaint.metachan)[threadi]
	ckvlen:= 0
	begin:
		for{
			select{
			case bufmeta := <- cd:
					kv,par,kvlen:= bufmeta.getKV(&rb.kvbuffer)
					ckvlen += kvlen
					//循环读取
					//par ,_:= bufmeta.BytesToInt()
					kvs:= string(kv)
					//Info.Printf("[DEBUG]线程xxx  %d  获得数据 %s 运行中!\n",threadi,kvs)
					rb.RingBuffWriteHandler(rb.sh,HANDLER,par,&kvs)
				//Info.Printf("xxxxxxxxxx  %d  获得数据 将要 释放锁! %v\n",threadi,bufmeta)
				//Info.Printf("[DEBUG]线程  %d  获得数据 释放锁!\n",threadi)
					//rb.sh.Handler(uint32(info[2]), []byte(key))
			default:
				//fmt.Printf("[DEBUG]线程 %d 运行中!\n",threadi)
				if len(cd) == 0  {
					time.Sleep(time.Microsecond * 20)
					if len(cd) == 0 {
						select{
						case isfinished := <- rb.chokesignal:
							if isfinished {
								if len(cd) == 0 {
									rb.lock2.Lock()
									//每次读完 标记下
									rb.bufmark += ckvlen
									rb.bufmark = (rb.bufmark) % len(rb.kvbuffer)
									rb.lock2.Unlock()
									goto theend
								}else{
									//Info.Printf("[DEBUG]线程 %d 休眠! 由于管道还有数据 故归还:%d \n",threadi,len(cd))
									rb.chokesignal<-isfinished
									goto begin
								}
							}
						default:
							goto begin
						}
					}
					//Info.Printf("[DEBUG]线程 %d 休眠! 管道数据:%d \n",threadi,len(cd))

					}

				}
			}
	theend:
}


func singlemonitor(group *sync.WaitGroup,bc *chan bool,chanlen int){
	group.Wait()
	for i:=0;i<chanlen;i++ {
		(*bc) <- true
	}
}

//执行溢出操作
func (rb *CacheBuf) SplliOut(){
	rb.spilloutime +=1
	if rb.spilloutdeadlock != 0  {
		//不为0 代表已经有线程正在等待了
		return
	}
	rb.wg3.Wait()
	rb.spilloutdeadlock += 1
	rb.wg3.Add(1)
	rb.spilloutdeadlock -= 1
	Info.Printf("Capacity Is Lower than 20%s Precent beging  %d times gc!","%",rb.spilloutime)
	//var keylen int
	//metapoints := len(rb.metaint.metapoint)
	//对数据进行排序
	go rb.sortMetaData()
	rb.wg2.Wait()
	//开一个携程 进行检测 排序内容是否全部输出完
	go singlemonitor(&rb.wg,&rb.chokesignal, int(rb.parnum))
	for  i:=0;i < len(*rb.metaint.metachan);i++ {
		rb.wg2.Add(1)
		go rb.WriteOutChan(i)
	}
	Info.Println("waiting spillout!")
	rb.wg.Wait()
	rb.wg2.Wait()
	for i:=0;i<len(*rb.metaint.metachan);i++{
		if len((*rb.metaint.metachan)[i])>0{
			panic("err")
		}
	}
	rb.RingBuffWriteHandler(rb.sh, WRITEOUTBUFF, 0, nil)
	Info.Println("sort finshed!")
	//fmt.Println(string(buff))
	//指针 buffer start 置为 end
	var tmpkvend int
	buflen := len(rb.kvbuffer)-1
	//与 kv区背靠背
	tmpkvend = rb.bufend - 1
	if tmpkvend < 0 {
		tmpkvend = -(-(tmpkvend) % (buflen+1)) + buflen + 1
	}
	rb.lock.Lock()
	Info.Println("[Info] Start Rinbuff Recycle!")
	Info.Println("Older Between New MetaInt Gap:",rb.metaint.getlen() - rb.metaint.metamark)
	if rb.metaint.getlen() - rb.metaint.metamark > 0 {
		rb.metaint.inital()
		tmpkvend = tmpkvend - METASIZE + 1
		for i := 0;i< rb.metaint.getlen() - rb.metaint.metamark ;i++ {
			md, err := rb.metaint.getInverse(i)
			if err != nil{
				Info.Println("Get MeatInt Error!")
				panic(err)
			}
			//rs,_ := md.getkey(&rb.kvbuffer)
			//fmt.Println("gc will relocalKey:",string(rs))
			if tmpkvend < 0 {
				a1 := -(-(tmpkvend) % (buflen + 1)) + buflen + 1
				c := 0
				for ix:= a1;ix< buflen+1;ix++{
					rb.kvbuffer[ix] = md[c]
					c++
				}
				cb := 0
				for iz:=c;iz<16;iz++{
					rb.kvbuffer[cb] = md[iz]
					cb++
				}
				tmpkvend = a1
			}else{
				for ia := 0;ia<METASIZE;ia++{
					rb.kvbuffer[tmpkvend + ia] = md[ia]
				}
			}
			tmpkvend = tmpkvend - METASIZE
		}
		rb.kvindex = tmpkvend

		rb.kvend = rb.kvindex + METASIZE -1
	}else{
		//tmpkvend 没有更新过
		rb.kvindex = tmpkvend - METASIZE + 1
		rb.kvend = tmpkvend
	}
	////检查输出的字节 数大小是否 和 index 和end大小 之间一致
	if rb.bufmark != rb.bufend {
		Info.Printf("Between Mark And End  Size Has %d Distance Shoudle Be Zero!\n",rb.bufend -rb.bufmark)
		panic("OutPut Size is not Match!\n")
	}
	rb.bufstart = rb.bufend
	//背对背 对齐 kv区
	rb.kvstart = rb.bufend - 1
	if rb.kvstart == -1 {
		rb.kvstart = buflen
	}
	//if rb.kvstart<10{
	//	fmt.Printf("==========>GC now rb.kvstart %d , rb.kvend  %d",rb.kvstart,rb.kvindex)
	//}
	Info.Println("[Info] Now Gc Finished!")
	rb.setTag(false)
	rb.lock.Unlock()
	rb.wg3.Done()
}

//对元数据区进行排序
func (rb *CacheBuf) sortMetaData()  {
	bufchan := *rb.metaint.metachan
	var tmp map[int][]*metadata
	tmp = make(map[int][]*metadata,0)
	for i:=0;i<len(rb.metaint.metapoint);i++{
		index := rb.metaint.metapoint[i].BytesToInt()
		if tmp[index[2]] == nil{
			tmp[index[2]] =make([]*metadata,0)
		}
		tmp[index[2]] = append(tmp[index[2]], rb.metaint.metapoint[i])
	}
	for i:=0;i<int(rb.parnum);i++ {
		//runtime.GOMAXPROCS(5) // 最多使用2个核
		//排序chan缓存数量 可以修改
		rb.wg.Add(1)
		tpe  := tmp[i]
		go Quicksort(i,rb.sortcache[i],&tpe,&rb.kvbuffer,&bufchan[i],&rb.wg)

	}
	rb.wg2.Done()
	rb.wg.Wait()
}
func(rb *CacheBuf) WriteOut(content *[]byte){
	buff := bytes.NewBuffer(rb.kvbuffer)
	n ,err:= io.ReadFull(buff,*content)
	if err != nil{
		panic(err)
	}
	if n != len(*content){
		Error.Println("error!")
	}
}

//计算剩余容量
func(rb *CacheBuf) surplusSpace() (float64,error) {
	kval :=rb.kvstart - rb.kvend
	if kval < 0 {
		kval = len(rb.kvbuffer) - rb.kvend  + rb.kvstart

	}
	bval := rb.bufindex - rb.bufstart
	if bval < 0 {
		bval = rb.bufindex + len(rb.kvbuffer) - rb.bufstart
	}
	val := len(rb.kvbuffer) - kval - bval
	//fmt.Printf("Residual byte:%d Usage ratio:%.0f %s \n",val,float64(val)/float64(len(rb.kvbuffer))* 100,"%")
	res:= float64(val)/float64(len(rb.kvbuffer))* 100
	if res < 0 {
		err := errors.New("capility overflow error!")
		return res,err
	}
	return res,nil
}

//插入 需要 值的长度 key
func(rb *CacheBuf) addmetadata(keyindex int,keyend int,vallen int,par uint32){
	//插入的起始 插入的 key结尾 插入的分区号 插入的 val长度
	bc := NewMetaData(keyindex,keyend,par,vallen)
	buflen := len(rb.kvbuffer) -1
	if rb.kvindex == -11{
		Info.Println("debug")
	}
	if rb.kvindex   < 0 {
		a1 := -(-(rb.kvindex) % (buflen+1) ) + buflen + 1
		c := 0
		for ix:= a1;ix< buflen+1;ix++{
			rb.kvbuffer[ix] = bc[c]
			c++
		}
		cb := 0
		for iz:=c;iz<METASIZE;iz++{
			rb.kvbuffer[cb] = bc[iz]
			cb++
		}
		rb.kvindex = a1
	}else{
		for ia := 0;ia<METASIZE;ia++{
			rb.kvbuffer[rb.kvindex + ia] = bc[ia]
		}
	}
	rb.kvend = rb.kvindex
	rb.kvindex = rb.kvindex - METASIZE
}