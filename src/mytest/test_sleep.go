package main

import (
"fmt"
	"math/rand"
	"time"
)

func main()  {
	fmt.Println("start")
	time.Sleep(time.Duration(rand.Intn(100))*time.Millisecond)
	fmt.Println("end")
}
