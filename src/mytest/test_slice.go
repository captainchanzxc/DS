package main

import "fmt"

func main(){
	var a= [4]int{1,2,3,4}
	for i,_:=range  a{
		fmt.Println(i)
	}
}
