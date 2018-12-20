package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"os"
	"raft"
	"strconv"
	"sync"
)

const Debug = 0

type OpType string

const (
	putOp    OpType = "put"
	appendOp        = "append"
	getOp           = "get"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      OpType
	Key       string
	Value     string
	SerialNum int
	ClerkId   int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	logFile    *os.File
	kvLog      *log.Logger
	mapDb      map[string]string
	serialNums map[int64]int
	readCh     chan string
	writeCompleteCh chan int

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Type: getOp, Key: args.Key, Value: "", SerialNum: args.SerialNum, ClerkId: args.ClerkId}
	_, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	if !isLeader {
		reply.Err = "kvserver is not a leader."
	} else {
		kv.kvLog.Printf("receive op: %v\n", op)
		reply.Value = <-kv.readCh
		kv.kvLog.Printf("reply op: %v\n", op)
	}
	kv.kvLog.Printf("return: %v\n", reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, SerialNum: args.SerialNum}
	if args.Op == "Put" {
		op.Type = putOp
	} else {
		op.Type = appendOp
	}
	if op.SerialNum <= kv.serialNums[op.ClerkId] {
		reply.Err = ""
		return
	}
	_, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	if !isLeader {
		reply.Err = "kvserver is not a leader."
	} else {
		kv.kvLog.Printf("receive op: %v\n", op)
		<-kv.writeCompleteCh
		kv.kvLog.Printf("reply op: %v\n", op)
	}

}

func (kv *KVServer) apply() {
	for true {
		kv.kvLog.Println("start to apply")
		applyMsg := <-kv.applyCh
		command := applyMsg.Command.(Op)
		kv.kvLog.Printf("apply: %v\n", command)
		_, isLeader := kv.rf.GetState()
		if command.Type == putOp || command.Type == appendOp {
			if kv.serialNums[command.ClerkId] < command.SerialNum {
				kv.serialNums[command.ClerkId] = command.SerialNum
				switch command.Type {
				case putOp:
					kv.mapDb[command.Key] = command.Value
					if isLeader{
						kv.writeCompleteCh<-1
					}
				case appendOp:
					kv.mapDb[command.Key] += command.Value
					if isLeader{
						kv.writeCompleteCh<-1
					}
				}

			}
		} else {
			if isLeader {
				kv.readCh <- kv.mapDb[command.Key]
			}
		}
		kv.kvLog.Println(kv.mapDb)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.logFile.Close()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	// You may need initialization code here.
	//kv.file="src/kvraft/"+strconv.Itoa(kv.me)+".txt"
	//_,err:=os.Create(kv.file)
	//if err!=nil{
	//	panic(err)
	//}
	f, err := os.Create(strconv.Itoa(kv.me) + ".log")
	kv.logFile=f
	if err != nil {
		panic(err)
	}
	kv.kvLog = log.New(kv.logFile, "[server "+strconv.Itoa(kv.me)+"] ", log.Lmicroseconds)

	kv.kvLog.Printf("servers number: %d\n", len(servers))
	kv.mapDb = make(map[string]string)
	kv.readCh = make(chan string)
	kv.writeCompleteCh=make(chan int)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.serialNums = make(map[int64]int)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()
	return kv
}
