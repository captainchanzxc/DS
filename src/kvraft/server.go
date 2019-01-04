package raftkv

import (
	"io/ioutil"
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
	serialNums      sync.Map
	applyReplyChMap sync.Map
	timeOut         time.Duration
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	clerkId := args.ClerkId
	kv.applyReplyChMap.LoadOrStore(clerkId, make(chan ApplyReplyArgs))

	op := Op{Type: getOp, Key: args.Key, Value: "", SerialNum: args.SerialNum, ClerkId: args.ClerkId, LeaderId: kv.me}
	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	if !isLeader {
		reply.Err = ERR_NOT_LEADER
	} else {
		kv.kvLog.Printf("receive get op: %v\n", op)
		ch, _ := kv.applyReplyChMap.Load(clerkId)
		applyReplyCh := ch.(chan ApplyReplyArgs)
		select {
		case applyReplyMsg := <-applyReplyCh:
			for applyReplyMsg.CommitIndex < index {
				kv.kvLog.Printf("apply an old cmd: %v\n", applyReplyMsg)
				select {
				case applyReplyMsg = <-applyReplyCh:
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
	kv.applyReplyChMap.LoadOrStore(clerkId, make(chan ApplyReplyArgs))
	kv.mu.Unlock()
	op := Op{Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, SerialNum: args.SerialNum, LeaderId: kv.me}
	if args.Op == "Put" {
		op.Type = putOp
	} else {
		op.Type = appendOp
	}
	serialNum, _ := kv.serialNums.LoadOrStore(op.ClerkId, 0)
	if op.SerialNum <= serialNum.(int) {
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
		ch, _ := kv.applyReplyChMap.Load(clerkId)
		applyReplyCh := ch.(chan ApplyReplyArgs)
		select {
		case applyReplyMsg := <-applyReplyCh:
			for applyReplyMsg.CommitIndex < index {
				kv.kvLog.Printf("apply an old cmd: %v\n", applyReplyMsg)
				select {
				case applyReplyMsg = <-applyReplyCh:
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
			kv.kvLog.Printf("install snapshot: %v\n", applyMsg.Snpst)
			kv.mu.Lock()
			kv.mapDb = applyMsg.Snpst.State
			snapShotSerialNums:=applyMsg.Snpst.SerialNums
			for k,v:=range snapShotSerialNums{
				kv.serialNums.Store(k,v)
			}
			kv.mu.Unlock()
		} else {
			command := applyMsg.Command.(Op)
			kv.kvLog.Printf("apply: %v\n", command)
			if kv.me == command.LeaderId {
				ch, _ := kv.applyReplyChMap.LoadOrStore(command.ClerkId, make(chan ApplyReplyArgs))
				applyReplyCh := ch.(chan ApplyReplyArgs)
				select {
				case applyReplyCh <- ApplyReplyArgs{Command: command, CommitIndex: applyMsg.CommandIndex, Value: kv.mapDb[command.Key]}:
				default:
				}
			}
			switch command.Type {
			case putOp:
				serialNum, _ := kv.serialNums.LoadOrStore(command.ClerkId, 0)
				if serialNum.(int) < command.SerialNum {
					kv.serialNums.Store(command.ClerkId, command.SerialNum)
					kv.mapDb[command.Key] = command.Value
					kv.kvLog.Printf("state size: %d, logs: %v\n", kv.rf.GetStateSize(), kv.rf.GetLogs())
					if kv.maxraftstate > 0 && kv.rf.GetStateSize() > kv.maxraftstate {
						kv.mu.Lock()
						snapShotSerialNums:=make(map[int64]int)
						kv.serialNums.Range(func(key, value interface{}) bool {
							snapShotSerialNums[key.(int64)]=value.(int)
							return true
						})
						snapShot := raft.SnapShot{LastIncludedIndex: applyMsg.CommandIndex, LastIncludedTerm: applyMsg.CommandTerm, State: kv.mapDb,SerialNums:snapShotSerialNums}
						kv.mu.Unlock()
						kv.kvLog.Printf("save snapshot: %v\n", snapShot)
						kv.rf.SaveSnapShot(snapShot)
					}

				}
			case appendOp:
				serialNum, _ := kv.serialNums.LoadOrStore(command.ClerkId, 0)
				if serialNum.(int) < command.SerialNum {
					kv.serialNums.Store(command.ClerkId, command.SerialNum)
					kv.mapDb[command.Key] += command.Value
					kv.kvLog.Printf("state size: %d, logs: %v\n", kv.rf.GetStateSize(), kv.rf.GetLogs())
					if kv.maxraftstate > 0 && kv.rf.GetStateSize() > kv.maxraftstate {
						kv.mu.Lock()
						snapShotSerialNums:=make(map[int64]int)
						kv.serialNums.Range(func(key, value interface{}) bool {
							snapShotSerialNums[key.(int64)]=value.(int)
							return true
						})
						snapShot := raft.SnapShot{LastIncludedIndex: applyMsg.CommandIndex, LastIncludedTerm: applyMsg.CommandTerm, State: kv.mapDb,SerialNums:snapShotSerialNums}
						kv.mu.Unlock()
						kv.kvLog.Printf("save snapshot: %v\n", snapShot)
						kv.rf.SaveSnapShot(snapShot)
					}
				}
			case getOp:
				kv.kvLog.Printf("state size: %d, logs: %v\n", kv.rf.GetStateSize(), kv.rf.GetLogs())
				if kv.maxraftstate > 0 && kv.rf.GetStateSize() > kv.maxraftstate {
					kv.mu.Lock()
					snapShotSerialNums:=make(map[int64]int)
					kv.serialNums.Range(func(key, value interface{}) bool {
						snapShotSerialNums[key.(int64)]=value.(int)
						return true
					})
					snapShot := raft.SnapShot{LastIncludedIndex: applyMsg.CommandIndex, LastIncludedTerm: applyMsg.CommandTerm, State: kv.mapDb,SerialNums:snapShotSerialNums}
					kv.mu.Unlock()
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
	fileName := strconv.Itoa(kv.me) + ".log"
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil && os.IsNotExist(err) {
		f, err = os.Create(fileName)
		if err != nil {
			panic(err)
		}
	}
	kv.logFile = f
	kv.kvLog = log.New(ioutil.Discard, "[server "+strconv.Itoa(kv.me)+"] ", log.Lmicroseconds)
	kv.kvLog.Printf("servers number: %d\n", len(servers))
	kv.applyCh = make(chan raft.ApplyMsg)
	//kv.snapShotSerialNums= make(map[int64]int)
	kv.timeOut = 3000 * time.Millisecond
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.RfLog.SetOutput(ioutil.Discard)
	snapShot,readOk := kv.rf.GetSnapShot()
	if readOk{
		kv.mapDb = snapShot.State
		snapShotSerialNums:=snapShot.SerialNums
		for k,v:=range snapShotSerialNums{
			kv.serialNums.Store(k,v)
		}
	}else {
		kv.mapDb=make(map[string]string)
	}

	kv.kvLog.Printf("[initial]rf.Logs: %v, rf.LastApplied: %d, rf.CommitIndex: %d, rf.LastIncludeIndex: %d"+
		"rf.LastIncludTerm: %d, kv.map: %v, kv.snapshotSerialNums: %v",
		kv.rf.Logs, kv.rf.LastApplied, kv.rf.CommitIndex, kv.rf.LastIncludedIndex, kv.rf.LastIncludedTerm, kv.mapDb,snapShot.SerialNums)
	// You may need initialization code here.
	go kv.apply()
	return kv
}
