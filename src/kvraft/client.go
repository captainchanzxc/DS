package raftkv

import (
	"io/ioutil"
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
	ckLog      *log.Logger
	logFile    *os.File
	lastLeader int
	serialNum  int
	id int64
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

	f, err := os.Create("client_0.log")
	ck.logFile =f
	if err != nil {
		panic(err)
	}
	ck.serialNum = 0
	ck.id=nrand()
	ck.ckLog = log.New(ioutil.Discard, "[client "+strconv.FormatInt(ck.id,10)+"] ", log.Lmicroseconds)
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
	ck.serialNum++
	args := GetArgs{Key: key, ClerkId: ck.id, SerialNum: ck.serialNum}
	//defer time.Sleep(3*time.Second)
	ck.ckLog.Printf("----------------------start to send get command: %v\n",args)
	reply := GetReply{}
	ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &args, &reply)
	if ok {
		if reply.Err == "" {
			ck.ckLog.Printf("send GetArgs: %v\n", args)
			ck.ckLog.Printf("receive ReplyArgs: %v\n", reply)
			return reply.Value
		}
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := GetReply{}
			//ck.ckLog.Printf("call server get %d\n",i)
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			//ck.ckLog.Printf("reply from server get %d\n",i)
			if ok {
				if reply.Err == "" {
					ck.ckLog.Printf("send GetArgs: %v, reply: %v\n", args,reply)
					ck.lastLeader = i
					return reply.Value
				} else {
					//	fmt.Printf("%d Error: %s\n",i,reply.Err)
				}
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
	ck.serialNum++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClerkId: ck.id, SerialNum: ck.serialNum}
	reply := PutAppendReply{}
	ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply)
	if ok {
		if reply.Err == "" {
			ck.ckLog.Printf("send PutAppendArgs: %v\n", args)
			return
		}
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := PutAppendReply{}
		//	ck.ckLog.Printf("call server put/append %d\n",i)
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		//	ck.ckLog.Printf("reply from server put/append %d\n",i)
			if ok {
				if reply.Err == "" {
					ck.ckLog.Printf("send PutAppendArgs: %v, reply: %v\n", args,reply)
					return
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
