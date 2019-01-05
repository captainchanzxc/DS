package main

import "fmt"

func main(){
	//var m=make(map[string]string)
	//for i:=0;i<4;i++{
	//	m[strconv.Itoa(i)]=strconv.Itoa(i)
	//}
	//for k,v:=range m{
	//	fmt.Println(k+":"+v)
	//}
	//m["100"]+="122"
	//fmt.Println(m["100"])
	m:=make(map[string]string)
	fmt.Println(len(m))
	m["0"]="0"
	fmt.Println(len(m))
}
