package raftkv

import (
	"labrpc"
	"log"
	"os"
	"strconv"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ckLog *log.Logger
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ckLog = log.New(os.Stdout, "[client "+strconv.Itoa(0)+"] ", log.Lmicroseconds)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//ck.servers[0].Call()
	args := GetArgs{Key: key}
	//defer time.Sleep(3*time.Second)
	ck.ckLog.Println("---start to send get command")
	valueCh:=make(chan string)
	for true {
		for i := 0; i < len(ck.servers); i++ {
			go func(server int) {
				reply := GetReply{}
				ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
				if ok {
					if reply.Err == "" {
						ck.ckLog.Printf("send GetArgs: %v\n", args)
						ck.ckLog.Printf("receive ReplyArgs: %v\n", reply)
						valueCh<-reply.Value
					} else {
						//	fmt.Printf("%d Error: %s\n",i,reply.Err)
					}
				}
			}(i)

			select {
			case value:=<-valueCh:
				return value
			default:


			}
		}
		time.Sleep(time.Duration(30) * time.Microsecond)
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == "" {
					ck.ckLog.Printf("send PutAppendArgs: %v\n", args)
					return
				} else {
				}
			}
		}
		time.Sleep(time.Duration(30) * time.Microsecond)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
