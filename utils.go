package src

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

func makeTimestamp(t int64) int64 {
	return t /int64(time.Millisecond)/1000
}

func formatTime(d time.Duration) string{
	a :=makeTimestamp(int64(d))
	return time.Unix(a,
		0).Format("2006-01-02 15:04:05")
}
func ReadFilex(filePth string, bufSize int) ([]byte,error) {
	content := make([]byte, 0) //一次读取多少个字节
	f, err := os.Open(filePth)
	if err != nil {
		return nil,err
	}
	defer f.Close()
	buf := make([]byte, bufSize) //一次读取多少个字节
	bfRd := bufio.NewReader(f)
	for {
		n, err := bfRd.Read(buf)
		if err != nil { //遇到任何错误立即返回，并忽略 EOF 错误信息
			if err == io.EOF {
				return content,nil
			}
			return nil,err
		}
		for m := range buf[:n]{
			content = append(content,buf[m] )
		}

	}

	return nil,nil
}
func WriteFile(filePth string,conetnt string){
	f, err := os.Create(filePth)
	if err != nil {
		fmt.Println(err)
		return
	}
	l, err := f.WriteString(conetnt)
	if err != nil {
		fmt.Println(err)
		f.Close()
		return
	}
	fmt.Println(l, "bytes written successfully")
	err = f.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
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
func IntToBytesU32(n uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf,n)
	return buf
}
func BytesToUint32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func read(offset uint32, end uint32, fileName string,counter int) ([]byte,error){
	size := 1024
	f,err:=os.Open(fileName)
	if err != nil{
		return nil,err
	}
	defer f.Close()
	if counter != 0{
		offset = uint32(size * counter)  + offset
	}

	if offset > end{
		return nil,io.EOF
	}
	//从头开始，文件指针偏移100
	f.Seek(int64(offset),0)

	len := (end - offset + 1 ) / uint32(size)
	if len <= 0 {
		size = int((end - offset + 1) % uint32(size + 1))
	}

	buffer:=make([]byte,size)
		// Read 后文件指针也会偏移
	n,err:=f.Read(buffer)

	if err == io.EOF {
		return nil,err
	}
	if err!=nil{
		panic(err)
	}
	return buffer[:n],nil

	//// 获取文件指针当前位置
	//cur_offset,_:=f.Seek(0,os.SEEK_CUR)
	//fmt.Printf("current offset is %d\n", cur_offset)
}

//希尔排序
func ShellSort(wg *sync.WaitGroup,a *[]string) {
	n := len(*a)
	h := 1
	for h < n/3 { //寻找合适的间隔h
		h = 3*h + 1
	}
	for h >= 1 {
		//将数组变为间隔h个元素有序
		for i := h; i < n; i++ {
			//间隔h插入排序
			for j := i; j >= h && CompareString(&(*a)[j],&(*a)[j-h]) == -1; j -= h {
				swap(a, j, j-h)
			}
		}
		h /= 3
	}
	wg.Done()
}
func swap(slice *[]string, i int, j int) {
	(*slice)[i], (*slice)[j] = (*slice)[j], (*slice)[i]
}

//比较2个字符串大小
func CompareString(a ,b *string) int {
	if len(*a) > len(*b){
		return 1
	}else if len(*a) == len(*b){
		for i:=0;i<len(*a);i++{
			if (*a)[i] == (*b)[i]{
				if i == len(*a)-1 {
					return 0
				}
				continue
			}else if (*a)[i] > (*b)[i]{
				return 1
			}else{
				return -1
			}
		}
	}
	return -1
}

//SHA256生成哈希值
func GetSHA256HashCode(message []byte)string{
	//方法一：
	//创建一个基于SHA256算法的hash.Hash接口的对象
	hash := sha256.New()
	//输入数据
	hash.Write(message)
	//计算哈希值
	bytes := hash.Sum(nil)
	//将字符串编码为16进制格式,返回字符串
	hashCode := hex.EncodeToString(bytes)
	//返回哈希值
	return hashCode

	//方法二：
	//bytes2:=sha256.Sum256(message)//计算哈希值，返回一个长度为32的数组
	//hashcode2:=hex.EncodeToString(bytes2[:])//将数组转换成切片，转换成16进制，返回字符串
	//return hashcode2
}
//判断 内容是否在 切片中
func Contains(container []string,key string) bool{
	for _,v :=  range container{
			if v == key{
				return true
			}
		}

		return false

}


