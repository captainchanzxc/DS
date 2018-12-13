package main

import "fmt"

func main(){
	var a []int
	for i:=0;i<4;i++{
		a=append(a,i)
	}
	fmt.Println(append(a,1,2,3,4,5))

	fmt.Printf("sss %v",a)
}
