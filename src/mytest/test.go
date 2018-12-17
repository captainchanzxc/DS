package main

import (
	"fmt"
	"strconv"
)

func main(){
	//var a int=5
	//var b int=3
	//fmt.Println(a/b)
	//fmt.Println(strconv.Itoa(1))
	//fmt.Println("ba"<"as")
	//s:=[]string{"1","2","3"}
	//for _,ts:=range s{
	//	fmt.Println(ts)
	//}
	//fmt.Println("s"=="s")
	//f:=func(c rune) bool {
	//	return !unicode.IsLetter(c)
	//}
	//res:=strings.FieldsFunc("213and;com,puter",f)
	//for _,r:=range res{
	//	fmt.Println(r)
	//}
	var s string
	s="132"+strconv.Itoa(112343)
	fmt.Println(s)
}