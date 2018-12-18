package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func main(){
	fileName:="./src/mytest/test.txt"
	//contents,err:=ioutil.ReadFile(fileName)
	//if err!=nil{
	//	fmt.Println("file not exist")
	//	fmt.Println(err)
	//	return
	//}
	//fmt.Println(contents)
	//fmt.Println(len(contents))
	//fmt.Println(string(contents))
	//
	//
	f,err:=os.Open(fileName)
	if err!=nil{
		panic(err)
	}
	rd:=bufio.NewReader(f)
	for {
		line,err:=rd.ReadString('\n')
		if err!=nil||io.EOF==err{
			break
		}
		fmt.Print(line)
	}

	//toFile:="./src/mytest/tfile.txt"



}
