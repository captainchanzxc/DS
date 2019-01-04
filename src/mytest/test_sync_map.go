package main

import (
	"fmt"
	"sync"
)

func main(){
	var m sync.Map
	m.Store("1","1v")
	m.Store("2","2v")
	v,_:=m.Load("1")
	fmt.Println(v=="1v")
	mm:=make(map[string]string)
	m.Range(func(key, value interface{}) bool {
		mm[key.(string)]=value.(string)
		return true
	})
	for k,v:=range mm{
		fmt.Println(k+v)
	}
}
