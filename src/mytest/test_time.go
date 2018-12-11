package main

import (
	"fmt"
	"time"
)

func main()  {
	t1:=time.Now().UnixNano()/int64(time.Millisecond)
	time.Sleep(100*time.Millisecond)
	var a int64
	a=1000
	t2:=time.Now().UnixNano()/int64(time.Millisecond)
	fmt.Println(t2-t1)
	fmt.Println(t2-a)
	fmt.Print(time.Now().Format("2006/01/02/ 15:03:04.000"))
}
