package main

import (
	"bufio"
	"os"
)

func main(){
	fileName:="src/mytest/test.txt"
	f,err:=os.OpenFile(fileName,os.O_APPEND|os.O_RDWR,0666)
	if err!=nil{
		panic(err)
	}
	defer f.Close()
	wr:=bufio.NewWriter(f)
	wr.WriteString("1234\n")
	wr.WriteString("12345\n")
	wr.Flush()
	_,err2:=os.Create("src/kvraft/test23.txt")
	if err!=nil{
		panic(err2)
	}
}
