package main

import (
	log2 "log"
	"os"
)

func main(){
	fileName:="src/raft/log.txt"
	f,err:=os.Create(fileName)
	if err!=nil{
		panic(err)
	}
	defer f.Close()

	log:=log2.New(f,"[info]",log2.LstdFlags)
	log.Println("1233")
}
