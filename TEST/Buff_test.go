package TEST

import (
	"bufio"
	"fmt"
	"github.com/stretchr/testify/assert"
	"godoop"
	"io"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"
)





func Test_metastruct(t *testing.T) {
	//xx := "caomaoboyhellomapreduce!"
	//rb :=NewCacheBuf(10)
	//rb.Write([]byte(xx))
	//buf := make([]byte,2)
	//rb.Read(buf)
	a := src.NewMetaData(999,56,34,21)
	fmt.Println(a)
	fmt.Println(a.BytesToInt())

}

func callbefore() *src.CacheBuf {
	a := src.NewCacheBuf(10,100)
	k1 := src.KeyValue{Key: "1", Value: "1234323"}
	k2 := src.KeyValue{Key: "2", Value: "12d323"}
	k3 := src.KeyValue{Key: "3", Value: "sdfsdf"}
	k4 := src.KeyValue{Key: "4", Value: "1sdfsdf2d323"}
	k5 := src.KeyValue{Key: "5", Value: "12ewrewd323"}
	k6 := src.KeyValue{Key: "6", Value: "12fsdgd323"}
	k7 := src.KeyValue{Key: "7", Value: "12dcxbx323"}

	a.collect(k1)
	a.collect(k2)
	a.collect(k3)
	a.collect(k4)
	a.collect(k5)
	a.collect(k6)
	a.collect(k7)

	return a
}

//测试 获取 metadata长度
func Test_getlen(t *testing.T){
	ts := assert.New(t)
	a := src.NewCacheBuf(10,1024)
	k1 := src.KeyValue{Key: "dsfsdf", Value: "1234323"}
	count := 20
	for m:=0;m<count;m++{
		a.collect(k1)
	}
	ts.Equal(count,a.metaint.getlen())
}
//用来测试 通过元索引取得的数据 是否和输入数据大小一致
//测试 无扩回收情况下
func Test_metadataGet(t *testing.T){
	ts := assert.New(t)
	a := src.NewCacheBuf(10,1024)
	count := 10
	keys := make([]src.KeyValue,0)
	for i:=0;i< count;i++{
		key := strconv.Itoa(i)
		keys= append(keys, src.KeyValue{Key: key, Value: "caomao"})
	}
	for _,val := range keys{
		if val.Key == "49"{
			fmt.Println("xxx")
		}
		a.collect(val)
	}
	resval := make([]string,0)
	for i,_:= range a.metaint.metapoint{
		xv := &a.metaint.metapoint[i]
		d,err := (*xv).getkey(&a.kvbuffer)
		if err != nil{
			panic(err)
		}
		resval = append(resval,string(d) )
	}
	num := 0
	for i,val := range resval{
		if keys[i].Key == val{
			num +=1
		}
	}
	ts.Equal(count,num)
}

//测试能否正常获取数组内的内容
func Test_ringGet(t *testing.T){
	a := callbefore()
	c,err := a.metaint.getInverse(0)
	if err != nil{
		panic(err)
	}

	if err != nil{
		panic(err)
	}
	fmt.Println(string(c))

}

func Test_ringbuf(t *testing.T) {
	a := callbefore()
	fmt.Println(a.metaint.getlen())
	c,err :=a.metaint.get(0)
	if err != nil{
		panic(err)
	}

	//for _,m:= range a.metaint.metapoint{
	//	c,_ :=m.BytesToInt()
	d,err := c.BytesToInt()
	if err != nil{
		panic(err)
	}
	src.formmat(d)
	//}
}

//↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
func testWirteOutBuff (sh *src.storeHandler,funtype int,par uint32,kv *string) {
	switch funtype {
	case src.HANDLER:
		path := "/Users/qiao/go/src/godoop/src/testout.txt" //写出测试文件的路径
		src.appendToFile(path,*kv)
		fmt.Println(*kv)
		return
	case src.MERGEFILE:
		return
	case src.WRITEOUTBUFF:
		return

	}
}
//测试 第一步 写出文件
func Test_ringBuffSpill(t *testing.T){
	path := "/Users/qiao/go/src/godoop/src/testout.txt" //写出测试文件的路径

	err := os.Remove(path)
	if err != nil {
		fmt.Println("can't find path error!")
	}
	//ts := assert.New(t)
	a := src.NewCacheBuf(10,1024 * 1 )
	//将上面测试处理器 设置到ringbuff
	a.SetCacheBuffWriteFunc(testWirteOutBuff)
	//写出从0 开始的键
	count := 2 << 7
	keys := make([]src.KeyValue,0)
	var key string
	for i:=0;i<count;i++ {
		key = strconv.Itoa(i)
		keys= append(keys, src.KeyValue{Key: key, Value: "caomao"})
	}

	for _,val := range keys{
		a.collect(val)
	}
	fmt.Println("finished")
	a.SpillAll()
	for _,val := range keys{
		a.collect(val)
	}
	fmt.Println("xxx")
	a.SpillAll()

}


//测试 ringbuff写出内容 第二步 是否和输入内容一致 需要修改ringbuff
func Test_ringbuffOut(t *testing.T) {
	count := 2 <<16
	xc := make(map[int]string ,0)
	repeat := 0
	for m:= 0;m<10;m++{
		path:= "/Users/qiao/go/src/godoop/src/testout.txt"
		a, err := src.ReadFilex(path,1024)
		if err != nil{
			panic(err)
		}
		strs := strings.Split(string(a),",")
		for _,i :=range strs{
			if len(i)== 0{
				continue
			}
			xa := strings.Split(i,":")
			index, err := strconv.Atoi(xa[0])
			if err != nil{
				panic(err)
			}
			_,ok := xc[index]
			if ok {
				repeat += 1
			}
			xc[index] =xa[1]
		}
	}
	for i := 0;i<count;i++{
		_,ok := xc[i]
		if len(xc) != count{
			panic("size error!")
		}
		if !ok{
			fmt.Println("nokey",i)
			panic("error")
		}
	}

	fmt.Println("实际输入:",count,"实际输出",len(xc),"测试通过!")
}
//↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑

//↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
//测试 偏移读写文件
func Test_reafile(t *testing.T){

	for i:=0;i<100;i++{
		counter:=0
		lenx := 0
		a:=rand.Intn(4000)//生成10以内的随机数
		b:=rand.Intn(4000)//生成10以内的随机数
		cx := a + b
		for{
			c,err:= src.read(uint32(a),uint32(cx),"/Users/qiao/go/src/godoop/src//map-spill-out-0",counter)
			if err != io.EOF && err != nil{
				panic(err)
			}
			if err != nil{
				fmt.Println("break")
				break
			}
			counter ++
			lenx += len(c)
			fmt.Println(string(c))
		}
		if lenx != b+1 {
			panic("error")
		}
	}

}

//测试 合并文件是否正确
func Test_mergefile(t *testing.T){
	//ts := assert.New(t)
	a := src.NewCacheBuf(10,1024 *1024  )
	a.SetCacheBuffWriteFunc(src.DefaultWirteOutBuff)
	count := 2 << 15
	keys := make([]src.KeyValue,0)
	var key string
	for i:=0;i<count;i++ {
		key = strconv.Itoa(i)
		keys= append(keys, src.KeyValue{Key: key, Value: "caomao"})
	}
	for _,val := range keys{
		if val.Key == "2028"{
			fmt.Println("xxx")
		}
		a.collect(val)
	}
	a.SpillAll()


	time.Sleep(time.Second * 2)
	xc := make(map[int]string ,0)

	for m:= 0;m<10;m++{
		path:= "/Users/qiao/go/src/godoop/src/map-out-%d"
		path =fmt.Sprintf(path,m)
		a, err := src.ReadFilex(path,1024)
		if err != nil{
			panic(err)
		}
		strs := strings.Split(string(a),",")
		for _,i :=range strs{
			if len(i)== 0{
				continue
		}
		xa := strings.Split(i,":")
		index, err := strconv.Atoi(xa[0])
		if err != nil{
			panic(err)
		}
		_,ok := xc[index]
		if ok {
			fmt.Println(index)
		}
		xc[index] =xa[1]
		}
	}

	fmt.Println(len(xc))
	for i := 0;i<count;i++{
		_,ok := xc[i]
		if len(xc) != count{
			panic("size error!")
		}
		if !ok{
			fmt.Println("nokey",i)
			panic("error")
		}
	}


}

//测试比较字符串大小
func Test_comparestring(t *testing.T){
	a := "1asdbs"
	b := "1asdas"
	if src.CompareString(&a,&b) != 1{
		panic("error")
	}
	a1 := "1asdbs"
	b2 := "1asdcs"
	if src.CompareString(&a1,&b2) != -1 {
		panic("error")
	}
	a3 := "1asdb"
	b4 := "1asdb"
	if src.CompareString(&a3,&b4) != 0{
		panic("error")
	}
}

func Test_xringbuff(t *testing.T) {
	a := src.NewRingbuffer(1024)
	xa := []byte("aasdasdasdasb")
	ok := a.Write(xa)
	buff := make([]byte,len(xa))
	fmt.Println(ok)
	for i,_ := range xa{
		fmt.Println(buff,i,a.GetUse())
		x := a.Read(buff)
		fmt.Println(x)
	}


}

func Test_slice(t *testing.T) {
	caomao := []byte("快速理解Go数组和切片的内部实现原理")
	ia := make([]byte,0)
	ia = append(ia, caomao[0])
	fmt.Println(unsafe.Sizeof(ia),&ia[0])

}

//读取外部数据文件 排序
func Test_getamountt( *testing.T) {

	file ,err:= os.Open("/Users/qiao/go/src/godoop/src/pg-huckleberry_finn.txt")
	if err != nil{
		panic(err)
	}
	count := 0
	defer file.Close()
	//a := NewCacheBuf(10,1024 *1024 * 500 )
	//a.SetCacheBuffWriteFunc(testWirteOutBuff)
	rf := bufio.NewReader(file)
	for{
		line,_,err := rf.ReadLine()
		if err == io.EOF{
			break
		}
		kv := src.Map("metamorphosis", string(line))
		//for _,m := range kv{
			count += len(kv)
			//a.collect(m)
		//}

	}
	fmt.Println("数据量大小",count)

}

//读取外部数据文件 排序
func Test_sort(t *testing.T) {

	file ,err:= os.Open("/Users/qiao/go/src/godoop/src/pg-metamorphosis.txt")
	if err != nil{
		panic(err)
	}
	count := 0
	defer file.Close()
	a := src.NewCacheBuf(10,1024 *1024 * 100)
	a.SetCacheBuffWriteFunc(src.DefaultWirteOutBuff)
	rf := bufio.NewReader(file)
	for{
		line,_,err := rf.ReadLine()
		if err == io.EOF{
			break
		}
		kv := src.Map("metamorphosis", string(line))
		for _,m := range kv{
		a.collect(m)
		}

	}
	a.SpillAll()
	fmt.Println("数据量大小",count)

}
