package main

import (
	"fmt"
	"time"
)

func main() {
	var tc = make(chan int)
	//var ac=make(chan  int)
	//go func() {
	//	select {
	//	case tc <- 1: //not execute tc<-1
	//		fmt.Println("tc")
	//	case ac<-1:
	//		fmt.Println("ac")
	//	fmt.Println("return")
	//	}
	//}()
	//time.Sleep(3*time.Second)
	//<-ac
	//time.Sleep(3*time.Second)
	//fmt.Println("exit")

	select {
	case tc <- 1:
	default:
		fmt.Println("default")
		break

	}

	go func() {
		time.Sleep(3 * time.Second)
		a := <-tc
		fmt.Println(a)
	}()
	time.Sleep(6 * time.Second)
	fmt.Println("return")
}
