package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"os"
	"raft"
	"strconv"
	"sync"
	"time"
)

const Debug = 0

type OpType string

const (
	putOp               OpType = "put"
	appendOp                   = "append"
	getOp                      = "get"
	ERR_CONNECTION_FAIL        = "connection fail"
	ERR_NOT_COMMIT             = "not commit"
	ERR_NOT_LEADER             = "not leader"
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
	LeaderId  int
}

type ApplyReplyArgs struct {
	CommitIndex int
	Command     Op
	Value       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	logFile         *os.File
	kvLog           *log.Logger
	mapDb           map[string]string
	serialNums      map[int64]int
	applyReplyChMap map[int64]chan ApplyReplyArgs
	timeOut         time.Duration
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	clerkId := args.ClerkId
	kv.mu.Lock()
	if kv.applyReplyChMap[clerkId] == nil {
		kv.applyReplyChMap[clerkId] = make(chan ApplyReplyArgs)
	}
	kv.mu.Unlock()

	op := Op{Type: getOp, Key: args.Key, Value: "", SerialNum: args.SerialNum, ClerkId: args.ClerkId, LeaderId: kv.me}
	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	if !isLeader {
		reply.Err = ERR_NOT_LEADER
	} else {
		kv.kvLog.Printf("receive get op: %v\n", op)
		select {
		case applyReplyMsg := <-kv.applyReplyChMap[clerkId]:
			for applyReplyMsg.CommitIndex < index {
				kv.kvLog.Printf("apply an old cmd: %v\n", applyReplyMsg)
				select {
				case applyReplyMsg = <-kv.applyReplyChMap[clerkId]:
				case <-time.After(kv.timeOut):
					kv.kvLog.Printf("time out: %v\n", op)
					reply.Err = ERR_NOT_COMMIT
					return
				}
			}
			kv.kvLog.Printf("command: %v, applyCommand: %v\n", op, applyReplyMsg.Command)
			kv.kvLog.Printf("command ID/index: %v/%v, apply ID/index: %v/%v\n",
				op.ClerkId, index, applyReplyMsg.Command.ClerkId, applyReplyMsg.CommitIndex)
			if index == applyReplyMsg.CommitIndex && applyReplyMsg.Command.ClerkId == op.ClerkId && applyReplyMsg.Command.SerialNum == op.SerialNum {
				kv.kvLog.Printf("reply get op succeed %v\n", applyReplyMsg)
				reply.Value = applyReplyMsg.Value
				kv.kvLog.Printf("send to client reply: %v\n", reply)
			} else {
				kv.kvLog.Printf("reply get op fail %v\n", applyReplyMsg)
				reply.Err = ERR_NOT_COMMIT
			}
		case <-time.After(kv.timeOut):
			kv.kvLog.Printf("time out: %v\n", op)
			reply.Err = ERR_NOT_COMMIT
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	clerkId := args.ClerkId
	kv.mu.Lock()
	if kv.applyReplyChMap[clerkId] == nil {
		kv.applyReplyChMap[clerkId] = make(chan ApplyReplyArgs)
	}
	kv.mu.Unlock()
	op := Op{Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, SerialNum: args.SerialNum, LeaderId: kv.me}
	if args.Op == "Put" {
		op.Type = putOp
	} else {
		op.Type = appendOp
	}
	if op.SerialNum <= kv.serialNums[op.ClerkId] {
		reply.Err = ""
		return
	}
	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	if !isLeader {
		reply.Err = ERR_NOT_LEADER
	} else {
		kv.kvLog.Printf("receive put/append op: %v\n", op)
		select {
		case applyReplyMsg := <-kv.applyReplyChMap[clerkId]:
			for applyReplyMsg.CommitIndex < index {
				kv.kvLog.Printf("apply an old cmd: %v\n", applyReplyMsg)
				select {
				case applyReplyMsg = <-kv.applyReplyChMap[clerkId]:
				case <-time.After(kv.timeOut):
					kv.kvLog.Printf("time out: %v\n", op)
					reply.Err = ERR_NOT_COMMIT
					return
				}
			}
			kv.kvLog.Printf("command: %v, applyCommand: %v\n", op, applyReplyMsg.Command)
			kv.kvLog.Printf("command ID/index: %v/%v, apply ID/index: %v/%v\n",
				op.ClerkId, index, applyReplyMsg.Command.ClerkId, applyReplyMsg.CommitIndex)
			if index == applyReplyMsg.CommitIndex && applyReplyMsg.Command.ClerkId == op.ClerkId && applyReplyMsg.Command.SerialNum == op.SerialNum {
				kv.kvLog.Printf("reply put/append op succeed %v\n", applyReplyMsg)
				kv.kvLog.Printf("send to client(put/append) reply: %v\n", reply)
			} else {
				reply.Err = ERR_NOT_COMMIT
				kv.kvLog.Printf("reply put/append op fail %v\n", applyReplyMsg)
			}
		case <-time.After(kv.timeOut):
			kv.kvLog.Printf("time out: %v\n", op)
			reply.Err = ERR_NOT_COMMIT
		}
	}
}

func (kv *KVServer) apply() {
	for true {
		kv.kvLog.Println("start to apply")
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid == false {
			//install snapshot
			kv.kvLog.Printf("install snapshot: %v\n",applyMsg.Snpst)
			kv.mapDb=applyMsg.Snpst.State
		} else {
			command := applyMsg.Command.(Op)
			kv.kvLog.Printf("apply: %v\n", command)
			if kv.me == command.LeaderId {
				select {
				case kv.applyReplyChMap[command.ClerkId] <- ApplyReplyArgs{Command: command, CommitIndex: applyMsg.CommandIndex, Value: kv.mapDb[command.Key]}:
				default:
				}
			}
			switch command.Type {
			case putOp:
				if kv.serialNums[command.ClerkId] < command.SerialNum {
					kv.serialNums[command.ClerkId] = command.SerialNum
					kv.mapDb[command.Key] = command.Value
					kv.kvLog.Printf("state size: %d, logs: %v\n", kv.rf.GetStateSize(), kv.rf.GetLogs())
					if kv.rf.GetStateSize() > kv.maxraftstate {
						snapShot := raft.SnapShot{LastIncludedIndex: applyMsg.CommandIndex, LastIncludedTerm: applyMsg.CommandTerm, State: kv.mapDb}
						kv.kvLog.Printf("save snapshot: %v\n", snapShot)
						kv.rf.SaveSnapShot(snapShot)
					}

				}
			case appendOp:
				if kv.serialNums[command.ClerkId] < command.SerialNum {
					kv.serialNums[command.ClerkId] = command.SerialNum
					kv.mapDb[command.Key] += command.Value
					kv.kvLog.Printf("state size: %d, logs: %v\n", kv.rf.GetStateSize(), kv.rf.GetLogs())
					if kv.rf.GetStateSize() > kv.maxraftstate {
						snapShot := raft.SnapShot{LastIncludedIndex: applyMsg.CommandIndex, LastIncludedTerm: applyMsg.CommandTerm, State: kv.mapDb}
						kv.kvLog.Printf("save snapshot: %v\n", snapShot)
						kv.rf.SaveSnapShot(snapShot)
					}
				}
			case getOp:
				kv.kvLog.Printf("state size: %d, logs: %v\n", kv.rf.GetStateSize(), kv.rf.GetLogs())
				if kv.rf.GetStateSize() > kv.maxraftstate {
					snapShot := raft.SnapShot{LastIncludedIndex: applyMsg.CommandIndex, LastIncludedTerm: applyMsg.CommandTerm, State: kv.mapDb}
					kv.kvLog.Printf("save snapshot: %v\n", snapShot)
					kv.rf.SaveSnapShot(snapShot)
				}
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
	kv.logFile = f
	if err != nil {
		panic(err)
	}
	kv.kvLog = log.New(kv.logFile, "[server "+strconv.Itoa(kv.me)+"] ", log.Lmicroseconds)
	kv.kvLog.Printf("servers number: %d\n", len(servers))
	kv.mapDb = make(map[string]string)
	kv.applyReplyChMap = make(map[int64]chan ApplyReplyArgs)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.serialNums = make(map[int64]int)
	kv.timeOut = 3000 * time.Millisecond
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.RfLog.SetOutput(kv.logFile)
	// You may need initialization code here.
	go kv.apply()
	return kv
}
