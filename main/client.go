
package main

import (
	. "6.824/src/mr"
)
func main(){
	wc := NewWorkerCli(10,Map,Reduce)
	wc.CliStart()


}
