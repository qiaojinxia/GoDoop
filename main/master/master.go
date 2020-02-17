package main

import (
	"6.824/src/mr"
	"strings"
	"time"
)

func main() {

	path := "map-out-pg-being_ernest.txt map-out-pg-dorian_gray.txt map-out-pg-frankenstein.txt pg-being_ernest.txt pg-dorian_gray.txt pg-frankenstein.txt"
	fils := strings.Split(path," ")
	filsx := make([]string, 0)
	a:="/Users/qiao/go/src/6.824/src/mr/main/"
	for _,v  := range fils{
		m := a + v
		filsx = append(filsx, m)
	}

	m := mr.MakeMaster(filsx, 10)
	m.Server()
	//for m.Done() == false {
	//	time.Sleep(time.Second)
	//}

	time.Sleep(time.Second)
}

