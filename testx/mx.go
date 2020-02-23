package main

import (
	"bufio"
	"fmt"
	"github.com/pkg/profile"
	src "godoop"
	"io"
	"os"
)

/**
 * Created by @CaomaoBoy on 2020/2/22.
 *  email:<115882934@qq.com>
 */

func main()  {
	defer profile.Start(profile.MemProfile).Stop()
	file ,err:= os.Open("/Users/qiao/go/src/godoop/pg-metamorphosis.txt")
	if err != nil{
		panic(err)
	}
	count := 0
	defer file.Close()
	a := src.NewCacheBuf(10,1024 *1024 * 10)
	sh := src.NewStoreHandler(10,"xxx")
	a.SetStoreHandler(sh)
	a.SetCacheBuffWriteFunc(src.DefaultWirteOutBuff)
	rf := bufio.NewReader(file)
	for{
		line,_,err := rf.ReadLine()
		if err == io.EOF{
			break
		}
		kv := src.Map("metamorphosis", string(line))
		for _,m := range kv{
			a.Collect(m)
		}

	}
	a.SpillAll()
	fmt.Println("数据量大小",count)
}