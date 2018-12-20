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
	ckLog      *log.Logger
	logFile    string
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
	ck.logFile = "client_0.log"
	_, err := os.Create(ck.logFile)

	if err != nil {
		panic(err)
	}
	ck.serialNum = 0
	ck.id=nrand()
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
	ck.serialNum++
	args := GetArgs{Key: key, ClerkId: ck.id, SerialNum: ck.serialNum}
	//defer time.Sleep(3*time.Second)
	ck.ckLog.Println("----------------------start to send get command")
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
			ck.ckLog.Println("call server get")
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			ck.ckLog.Println("reply from server get")
			if ok {
				if reply.Err == "" {
					ck.ckLog.Printf("send GetArgs: %v\n", args)
					ck.ckLog.Printf("receive ReplyArgs: %v\n", reply)
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
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == "" {
					ck.ckLog.Printf("send PutAppendArgs: %v\n", args)
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
