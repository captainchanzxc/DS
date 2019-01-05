package main

import "fmt"

type TestM struct {
	A bool
	M map[int]int
}

func main() {
	m:=make(map[int]int)
	m[1]=1
	t := TestM{A: true,M:m}
	b := t
	b.A = false
	b.M[1]=2
	fmt.Println(t)
	fmt.Println(b)
}
