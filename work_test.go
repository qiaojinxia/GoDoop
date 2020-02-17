package src

import "testing"

func Test_work(t *testing.T){
	wc := NewWorkerCli(1, Map, Reduce)
	wc.Working()
}
