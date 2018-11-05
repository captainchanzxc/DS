package main

import "fmt"

func test(){
	go func() {
		for i:=1;i<10000;i++{
			fmt.Printf("goroutine+%d\n",i)
		}
	}()
	for i:=1;i<100;i++{
		fmt.Printf("test+%d\n",i)
	}
}

func main(){
	test()
	for i:=1;i<100;i++{
		fmt.Printf("main+%d\n",i)
	}
	fmt.Println("main exit")
}
