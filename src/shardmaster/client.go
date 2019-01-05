package shardmaster

//
// Shardmaster clerk.
//

import (
	"io/ioutil"
	"labrpc"
	"log"
	"os"
	"strconv"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id int64
	serialNum int

	ckLog      *log.Logger
	logFile    *os.File
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
	// Your code here.
	ck.id=nrand()
	ck.serialNum=0
	f, err := os.Create("client_0.log")
	ck.logFile =f
	if err != nil {
		panic(err)
	}
	ck.ckLog = log.New(ioutil.Discard, "[client "+strconv.FormatInt(ck.id,10)+"] ", log.Lmicroseconds)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.serialNum++
	args.SerialNum=ck.serialNum
	args.ClerkId=ck.id
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false &&reply.Err=="" {
				ck.ckLog.Printf("query receive: %v\n",reply)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClerkId=ck.id
	ck.serialNum++
	args.SerialNum=ck.serialNum

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false &&reply.Err==""{
				ck.ckLog.Printf("send JoinArgs: %v\n",args)
				ck.ckLog.Printf("join receive: %v\n",reply)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	ck.serialNum++
	args.SerialNum=ck.serialNum
	args.ClerkId=ck.id
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false&&reply.Err=="" {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	ck.serialNum++
	args.SerialNum=ck.serialNum
	args.ClerkId=ck.id
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false &&reply.Err==""{
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
