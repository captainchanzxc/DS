package main

import (
	"fmt"
	"sort"
)

type KV struct {
	Key string
	Value string
}


type KVS []KV
func (a KVS) Len() int { return len(a) }
func (a KVS) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a KVS) Less(i, j int) bool { return a[i].Key<a[j].Key }
func main(){
	kvs:=KVS{{"2","222"},{"1","111"},{"3","333"}}
	sort.Sort(kvs)
	for i:=0;i<3;i++{
		fmt.Println(kvs[i].Key,kvs[i].Value)
	}

	var t sort.StringSlice
	t=append(t, "cd")
	t=append(t, "ad")
	t=append(t, "ac")
	sort.Sort(t)
	a:="1"
	for i,r:=range  t{
		fmt.Println(i)
		fmt.Println(a+r)
	}
}
