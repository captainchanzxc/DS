package main

import "fmt"

type key struct {
	id int
	name int
}
func main(){
	key1:=key{id:1,name:1}
	key2:=key{id:2,name:2}
	m:=make(map[key]int)
	m[key1]=1
	m[key2]=2
	m[key1]+=1
	fmt.Println(&key1)
}
