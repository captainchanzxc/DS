package main

import (
	"encoding/json"
	"os"
	"strconv"
)
type Test struct{
	Id string
	Age int
}

func main(){
	path:="./src/mytest/test.txt"
	f,_:=os.Create(path)

	enc:=json.NewEncoder(f)
	var t []Test
	for a:=0; a<5; a++{
		m:=Test{strconv.Itoa(a),a}
		t=append(t,m)
	}
	enc.Encode(t[0])
	enc.Encode(t[1])
	f.Close()
	//
	//ff,_:=os.Open(path)
	//
	//dec:=json.NewDecoder(ff)
	//var dv Test
	//var err error
	//err=dec.Decode(&dv)
	//fmt.Println(err)
	//fmt.Println(dv.Age)
	//fmt.Println(dv.Id)
	//
	//err=dec.Decode(&dv)
	//fmt.Println(err)
	//fmt.Println(dv.Age)
	//fmt.Println(dv.Id)
	//
	//err=dec.Decode(&dv)
	//fmt.Println(err)
	//fmt.Println(dv.Age)
	//fmt.Println(dv.Id)
}