package src

import "testing"

func Test_work(t *testing.T){
	wc := NewWorkerCli(10, Map, Reduce)
	wc.CliStart()
}
