package main

import (
	"fmt"
	"sync"
)

func main(){
	var m sync.Map
	m.Store("1","1v")
	v,_:=m.Load("1")
	fmt.Println(v=="1v")
}
