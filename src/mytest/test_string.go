package main

import (
	"fmt"
	"strings"
)

func main()  {
	a:="12345,123"
	fmt.Println(a[0:len(a)-1])
	b:=strings.Split(a,",")
	for _,c:=range b{
		fmt.Println(c)
	}
}
