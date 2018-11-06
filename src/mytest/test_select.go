package main

import (
	"fmt"
	"time"
)

func main(){
	var tc=make(chan  int)
	var ac=make(chan  int)
	go func() {
		select {
		case tc <- 1: //not execute tc<-1
			fmt.Println("tc")
		case ac<-1:
			fmt.Println("ac")
		fmt.Println("return")
		}
	}()
	time.Sleep(3*time.Second)
	<-ac
	time.Sleep(3*time.Second)
	fmt.Println("exit")

}
