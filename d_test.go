package src

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"testing"
)

/**
 * Created by @CaomaoBoy on 2020/2/16.
 *  email:<115882934@qq.com>
 */

func Test_sort(t *testing.T){
	file ,err:= os.Open("/Users/qiao/go/src/godoop/pg-metamorphosis.txt")
	if err != nil{
		panic(err)
	}
	count := 0
	defer file.Close()
	a := NewCacheBuf(10,1024 *1024 * 100)
	a.SetCacheBuffWriteFunc(DefaultWirteOutBuff)
	rf := bufio.NewReader(file)
	for{
		line,_,err := rf.ReadLine()
		if err == io.EOF{
			break
		}
		kv := Map("metamorphosis", string(line))
		for _,m := range kv{
			a.Collect(m)
		}

	}
	a.SpillAll()

	fmt.Println("数据量大小",count)

}