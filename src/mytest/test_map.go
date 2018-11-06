package main

import (
	"fmt"
	"strconv"
)

func main(){
	var m=make(map[string]int)
	for i:=0;i<4;i++{
		m[strconv.Itoa(i)]=i
	}
	for k,v:=range m{
		fmt.Println(k+":"+strconv.Itoa(v))
	}
}
