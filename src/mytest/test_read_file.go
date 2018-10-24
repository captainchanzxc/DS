package main

import (
	"fmt"
	"io/ioutil"
)

func main(){
	fileName:="./inter_files/rr.txt"
	contents,err:=ioutil.ReadFile(fileName)
	if err!=nil{
		fmt.Println("file not exist")
		fmt.Println(err)
		return
	}
	fmt.Println(contents)
	fmt.Println(len(contents))
	fmt.Println(string(contents))

	//toFile:="./src/mytest/tfile.txt"



}
