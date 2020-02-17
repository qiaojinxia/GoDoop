package src

import (
	"math/rand"
	"sync"
	time2 "time"
)

/**
 * Created by @CaomaoBoy on 2020/2/15.
 *  email:<115882934@qq.com>
 */

func Quicksort(i int,cache map[string]map[string]int,arr *[] *metadata,kvbuffer *[]byte,bufchan *chan *metadata,wg *sync.WaitGroup){
	bg := time2.Now()
	QuickSort(arr,cache,0,len(*arr)-1,kvbuffer)
	ed := time2.Now().Sub(bg).Microseconds()
	qps:= float64(ed)/float64(len(*arr))
	Info.Printf("线程 %d 完成 对%d条数据进行排序 Qps :%.2f(微秒/条) 数据将 写入管道中... ",i,len(*arr),qps)
	for _,v := range *arr{
		*bufchan <-  v
		//Info.Println(fmt.Sprintf("线程 %d 长度 %d 写入................\n",i,len(*bufchan)))
	}
		//Info.Printf("线程 %d 写入完成 管道 %d 退出 ................\n",i,len(*bufchan))
	wg.Done()
}

func SortMerge(arr *[] *metadata,cache map[string]map[string]int,left int,right int,kvbuffer *[]byte){
	for i:= left;i<=right;i++{
		tmp := (*arr)[i]
		var j int
		for j=i;j>left && (*arr)[j-1].compar(cache,kvbuffer,tmp.getkey(kvbuffer)) == 1;j--{
			(*arr)[j] = (*arr)[j-1]
		}
		(*arr)[j] = tmp
	}
}
func swapx(arr *[] *metadata ,j int,i int){
	(*arr)[i],(*arr)[j] = (*arr)[j],(*arr)[i]
}
func QuickSort(arr *[] *metadata,cache map[string]map[string]int,left int,right int,kvbuffer *[]byte){
	if right - left < 20 {
		SortMerge(arr,cache,left,right,kvbuffer)
	}else{
		swapx(arr,left,rand.Int()%(right - left +1) + left)
		vdata := (*arr)[left]
		lt := left
		gt := right + 1
		i := left + 1
		for i < gt {
			if (*arr)[i].compar(cache,kvbuffer,vdata.getkey(kvbuffer)) == -1{
				swapx(arr,i,lt + 1)
				lt ++
				i ++
			}else if (*arr)[i].compar(cache,kvbuffer,vdata.getkey(kvbuffer)) == 1{
				swapx(arr,i,gt -1)
				gt --
			}else{
				i ++

			}
		}
		swapx(arr,left,lt)
		QuickSort(arr,cache,left,lt-1,kvbuffer)
		QuickSort(arr,cache,gt,right,kvbuffer)
	}
}