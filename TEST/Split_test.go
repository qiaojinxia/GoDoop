package TEST

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func Test_split(t *testing.T) {


		file, err := os.Open("/Users/qiao/go/src/6.824/src/main/pg-sherlock_holmes.txt")
		if err != nil {
			fmt.Println("failed to open:", err)
		}

		defer file.Close()

		SplitFilex(file, 1024)


}
type xxa struct {
	caomao string
}
func Test_aaas(t *testing.T){
	tesr1 := make(chan *xxa,10)
	xx :=make(map[int]xxa,0)
	b :=xxa{caomao:"XXb"}
	c :=xxa{caomao:"XXc"}
	d :=xxa{caomao:"XXd"}
	e :=xxa{caomao:"XXe"}

	xx[1] =b
	xx[2] =c
	xx[3] =d
	xx[4] =e
	for i,v := range xx{
		fmt.Println("v的值:",v)
		tmp := xx[i]
		tesr1 <- &tmp
	}

	for m := range tesr1{
		fmt.Println(m)
	}
}



func Test_append(t *testing.T) {

	appendToFile("caomao.txt","123123,,")

}
func appendToFile(file, str string) {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0660)
	if err != nil {
		fmt.Printf("Cannot open file %s!\n", file)
		return
	}
	defer f.Close()
	f.WriteString(str)
}




func SplitFilex(file *os.File,size int) {
	finfo, err := file.Stat()
	if err != nil {
		fmt.Println("get File info failed", file, size)
		return
	}
	//打印文件信息
	fmt.Println(finfo, size)
	bufsize := 2 << 19 // 默认 1024 * 1024 1M大小
	//如果小于 1M那么 就为要读文件的大小
	if size < bufsize {
		bufsize = size
	}
	//定义读取的 容器
	buf := make([]byte, bufsize)
	num := (int(finfo.Size()) + size - 1) / size //计算分块数量
	fmt.Println(num, len(buf))
	for i := 0; i < num; i++ {
		copylen := 0
		//文件名.txt + 块编号{0}
		newfilename := finfo.Name() + strconv.Itoa(i)
		newfile, err1 := os.Create(newfilename)
		if err1 != nil {
			fmt.Println("failed to create file!", newfilename)
		} else {
			fmt.Println("create file:", newfilename)
		}
		for copylen < size {
			n, err2 := file.Read(buf)
			if err2 != nil && err2 != io.EOF {
				fmt.Println(err2, "failed to read from:", file)
				break
			}
			if n <= 0 {
				break
			}
			w_buf := buf[:n]
			newfile.Write(w_buf)
			copylen += n
		}
	}
	return
}
func Test_xxx(t *testing.T) {

	m := []byte{'a','x'}
	var a   *[]byte
	var xx []byte
	a =& m
	xx = *a

	fmt.Println(strings.Compare("123","1"))
	fmt.Println(xx[0:2])

	x()
}

func makeTimestamp(t int64) int64 {

	return t /int64(time.Millisecond ) /1000
}
func x(){
	t := time.Duration(time.Now().UnixNano())
	ts := makeTimestamp(int64(t))
	fmt.Println(ts)
	fmt.Println(time.Unix(ts,
		0).Format("2006-01-02 15:04:05"))

}


func IntToBytesU32(n uint32) []byte {
	data := uint32(n)
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, data)
	return bytebuf.Bytes()
}

func BytesToUint32(bys []byte) uint32 {
	bytebuff := bytes.NewBuffer(bys)
	var data uint32
	binary.Read(bytebuff, binary.BigEndian, &data)
	return data
}


//测试存储2 uint32 成8个字节 和恢复
func Test_mapappend(t *testing.T){
	var a uint32
	var a1 uint32
	a = 234
	a1 = 4566
	b := IntToBytesU32(a)
	b1 := IntToBytesU32(a1)
	c := make([]byte,0)
	c = append(c, b...)
	c = append(c, b1...)
	o1 := BytesToUint32(c[:4])
	o2 := BytesToUint32(c[4:])
	fmt.Println(o1,o2)

}
//测试 map取出的值 是值传递
func Test_StructRef(t *testing.T){
	type B struct {
		s string
	}

	type  A struct{
		a1 int
		m map[int]B
	}

	var m map[int]A
	m = make(map[int]A,0)
	s := A{
		a1: 21,
		m:  make(map[int]B,0),
	}
	m[1] = s



}
//测试 一个 汉子占3个字节
func Test_StringLen(t *testing.T){
	a := []byte("草帽")
	fmt.Println(a)
}

//测试 当 map 发生变化 map 指向的是不是新的map地址
func Test_mapchange(t *testing.T){
	a := make([]byte,0)
	b := &a
	c := &b
	d := &c
	fmt.Println(***d,c)
	a = append(a, []byte("123123")...)
	fmt.Println(&c)
	a = append(a, []byte("12312aaaaaaaaaaaaaaaaaaaaa3")...)
	fmt.Println(*b)
}

//测试 一个 汉子占3个字节
func Test_StrixngLen(t *testing.T){


	a := []byte{0,3,203,239}
	ax := BytesToUint32(a)
	fmt.Println(ax)
}

//测试 一个 汉子占3个字节
func Test_ChanRef(t *testing.T){

	ax := make([]chan *[]byte,1)
	bx := make(chan *[]byte,2)
	a := []byte{0,3,203,239}
	bx <- &a
	ax[0] =bx
	fmt.Println("xsaxas")

}

type asd struct {
	testing string
	wg sync.WaitGroup

}
func Newasd() *asd{
	return &asd{
		testing: "asd",
		wg:      sync.WaitGroup{},
	}
}

func (a *asd) Newasd() {
	for i := 0; i < 5; i++ {
		a.wg.Add(1)
		go a.xxxx(i)
	}
	a.wg.Wait()
}

func(a *asd) xxxx(i int) {
		log.Printf("i:%d", i)
		a.wg.Done()
}

func Test_Cxxxf(t *testing.T){
	binary := Newasd()
	binary.Newasd()
}
//管道先入先出结构
func Test_chanst(t *testing.T) {
	cn := make(chan int,10)
	for i:=0;i<10;i++ {
		cn<- i
	}
	for n:= range cn{
		fmt.Println(n)
	}

}

func Test_slice(t *testing.T) {
	//排序缓存
	tmp := make(map[*[]byte]map[*[]byte]int,0)
	sl := make([]map[*[]byte]map[*[]byte]int,0)
	tm := make(map[*[]byte]int,0)
	key := []byte("xxx")
	tm[&key] = 1
	bbb := []byte("xxx")
	tm[&bbb] = 2
	sl = append(sl, tmp)
	//sa(sl)
	fmt.Println(sl)

}

func sa(a []map[string]map[string]int) {
	fmt.Println(a)
	tm := make(map[string]int,0)
	tm["xxxx"] = 2
	xx := a[0]
	xx["aaa"]= tm


}

