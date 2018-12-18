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
	Type  OpType
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	logFile string
	kvLog   *log.Logger
	mapDb   map[string]string
	readCh  chan string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Type: getOp, Key: args.Key, Value: ""}
	_, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	reply.Value = <-kv.readCh
	if !isLeader {
		reply.Err = "kvserver is not a leader."
	} else {
	}
	kv.kvLog.Printf("return: %v \n", reply)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Key: args.Key, Value: args.Value}

	if args.Op == "Put" {
		op.Type = putOp
	} else {
		op.Type = appendOp
	}
	_, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	if !isLeader {
		reply.Err = "kvserver is not a leader."
	}

}

func (kv *KVServer) apply() {
	for true {
		kv.kvLog.Println("start to apply")
		applyMsg := <-kv.applyCh
		command := applyMsg.Command.(Op)
		kv.kvLog.Printf("apply: %v\n", command)

		//kv.kvLog.Println("lock mapDb")
		switch command.Type {
		case putOp:
			kv.mapDb[command.Key] = command.Value
		case appendOp:
			kv.mapDb[command.Key] += command.Value
		case getOp:
			kv.readCh <- kv.mapDb[command.Key]
		}
		kv.kvLog.Println(kv.mapDb)
		//kv.kvLog.Println("unlock mapDb")
	}
}

//func (kv *KVServer) putToFile(key string, value string) {
//	//f, err := os.OpenFile(kv.file,os.O_APPEND|os.O_RDWR,0666)
//	//if err != nil {
//	//	panic(err)
//	//}
//	//defer f.Close()
//	//rd := bufio.NewReader(f)
//	//kvMap := make(map[string]string)
//	//for {
//	//	line, err := rd.ReadString('\n')
//	//	if err != nil || io.EOF == err {
//	//		break
//	//	}
//	//	line = line[0 : len(line)-1]
//	//	keyValue := strings.Split(line, ",")
//	//	kvMap[keyValue[0]] = keyValue[1]
//	//}
//	//kvMap[key]=value
//	//wr:=bufio.NewWriter(f)
//	//for k,v:=range kvMap{
//	//	wr.WriteString(k+","+v+"\n")
//	//}
//	//wr.Flush()
//}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
	kv.logFile = strconv.Itoa(kv.me) + ".log"
	f, err := os.Create(kv.logFile)
	if err != nil {
		panic(err)
	}
	kv.kvLog = log.New(f, "[server "+strconv.Itoa(kv.me)+"] ", log.Lmicroseconds)

	kv.kvLog.Printf("servers number: %d\n", len(servers))
	kv.mapDb = make(map[string]string)
	kv.readCh = make(chan string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()

	return kv
}
