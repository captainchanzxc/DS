package shardmaster

import (
	"io/ioutil"
	"log"
	"os"
	"raft"
	"sort"
	"strconv"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

type OpType string

const (
	joinOp              OpType = "join"
	leaveOp                    = "leave"
	moveOp                     = "move"
	queryOp                    = "query"
	ERR_CONNECTION_FAIL        = "connection fail"
	ERR_NOT_COMMIT             = "not commit"
	ERR_NOT_LEADER             = "not leader"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      OpType
	SerialNum int
	ClerkId   int64
	LeaderId  int

	//JoinArgs
	Servers map[int][]string
	//LeaveArgs
	GIDs []int
	//MoveArgs
	Shard int
	GID   int
	//QueryArgs
	Num int
}

type ApplyReplyArgs struct {
	CommitIndex   int
	Command       Op
	Configuration Config
}
type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	// Your definitions here.
	logFile         *os.File
	smLog           *log.Logger
	serialNums      sync.Map
	applyReplyChMap sync.Map
	timeOut         time.Duration
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	clerkId := args.ClerkId
	sm.applyReplyChMap.LoadOrStore(clerkId, make(chan ApplyReplyArgs))

	op := Op{ClerkId: args.ClerkId, SerialNum: args.SerialNum, LeaderId: sm.me, Type: joinOp, Servers: args.Servers}
	serialNum, _ := sm.serialNums.LoadOrStore(op.ClerkId, 0)
	if op.SerialNum <= serialNum.(int) {
		reply.Err = ""
		return
	}
	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	if !isLeader {
		reply.Err = ERR_NOT_LEADER
	} else {
		sm.smLog.Printf("receive JoinArgs: %v\n", args)
		ch, _ := sm.applyReplyChMap.Load(clerkId)
		applyReplyCh := ch.(chan ApplyReplyArgs)
		select {
		case applyReplyMsg := <-applyReplyCh:
			for applyReplyMsg.CommitIndex < index {
				sm.smLog.Printf("apply an old cmd: %v\n", applyReplyMsg)
				select {
				case applyReplyMsg = <-applyReplyCh:
				case <-time.After(sm.timeOut):
					//sm.smLog.Printf("time out: %v\n", op)
					reply.Err = ERR_NOT_COMMIT
					return
				}
			}

			if index == applyReplyMsg.CommitIndex && applyReplyMsg.Command.ClerkId == op.ClerkId && applyReplyMsg.Command.SerialNum == op.SerialNum {
				sm.smLog.Printf("join reply %v\n", applyReplyMsg)
			} else {
				reply.Err = ERR_NOT_COMMIT
				sm.smLog.Printf("reply put/append op fail %v\n", applyReplyMsg)
			}
		case <-time.After(sm.timeOut):
			sm.smLog.Printf("Join time out: %v\n", op)
			reply.Err = ERR_NOT_COMMIT
		}
	}

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	clerkId := args.ClerkId
	sm.applyReplyChMap.LoadOrStore(clerkId, make(chan ApplyReplyArgs))

	op := Op{ClerkId: args.ClerkId, SerialNum: args.SerialNum, LeaderId: sm.me, Type: leaveOp, GIDs: args.GIDs}
	serialNum, _ := sm.serialNums.LoadOrStore(op.ClerkId, 0)
	if op.SerialNum <= serialNum.(int) {
		reply.Err = ""
		return
	}
	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	if !isLeader {
		reply.Err = ERR_NOT_LEADER
	} else {
		sm.smLog.Printf("receive LeaveArgs: %v\n", args)
		ch, _ := sm.applyReplyChMap.Load(clerkId)
		applyReplyCh := ch.(chan ApplyReplyArgs)
		select {
		case applyReplyMsg := <-applyReplyCh:
			for applyReplyMsg.CommitIndex < index {
				sm.smLog.Printf("apply an old cmd: %v\n", applyReplyMsg)
				select {
				case applyReplyMsg = <-applyReplyCh:
				case <-time.After(sm.timeOut):
					sm.smLog.Printf("time out: %v\n", op)
					reply.Err = ERR_NOT_COMMIT
					return
				}
			}
			//sm.smLog.Printf("command: %v, applyCommand: %v\n", op, applyReplyMsg.Command)
			//sm.smLog.Printf("command ID/index: %v/%v, apply ID/index: %v/%v\n",
			//op.ClerkId, index, applyReplyMsg.Command.ClerkId, applyReplyMsg.CommitIndex)
			if index == applyReplyMsg.CommitIndex && applyReplyMsg.Command.ClerkId == op.ClerkId && applyReplyMsg.Command.SerialNum == op.SerialNum {
				//sm.smLog.Printf("reply put/append op succeed %v\n", applyReplyMsg)
				//sm.smLog.Printf("send to client(put/append) reply: %v\n", reply)
			} else {
				reply.Err = ERR_NOT_COMMIT
				sm.smLog.Printf("reply put/append op fail %v\n", applyReplyMsg)
			}
		case <-time.After(sm.timeOut):
			sm.smLog.Printf("Leave time out: %v\n", op)
			reply.Err = ERR_NOT_COMMIT
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// Your code here.
	clerkId := args.ClerkId
	sm.applyReplyChMap.LoadOrStore(clerkId, make(chan ApplyReplyArgs))

	op := Op{ClerkId: args.ClerkId, SerialNum: args.SerialNum, LeaderId: sm.me, Type: moveOp, Shard: args.Shard, GID: args.GID}
	serialNum, _ := sm.serialNums.LoadOrStore(op.ClerkId, 0)
	if op.SerialNum <= serialNum.(int) {
		reply.Err = ""
		return
	}
	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	if !isLeader {
		reply.Err = ERR_NOT_LEADER
	} else {
		sm.smLog.Printf("receive MoveArgs: %v\n", args)
		ch, _ := sm.applyReplyChMap.Load(clerkId)
		applyReplyCh := ch.(chan ApplyReplyArgs)
		select {
		case applyReplyMsg := <-applyReplyCh:
			for applyReplyMsg.CommitIndex < index {
				sm.smLog.Printf("apply an old cmd: %v\n", applyReplyMsg)
				select {
				case applyReplyMsg = <-applyReplyCh:
				case <-time.After(sm.timeOut):
					sm.smLog.Printf("time out: %v\n", op)
					reply.Err = ERR_NOT_COMMIT
					return
				}
			}
			if index == applyReplyMsg.CommitIndex && applyReplyMsg.Command.ClerkId == op.ClerkId && applyReplyMsg.Command.SerialNum == op.SerialNum {
				//sm.smLog.Printf("reply put/append op succeed %v\n", applyReplyMsg)
				//sm.smLog.Printf("send to client(put/append) reply: %v\n", reply)
			} else {
				reply.Err = ERR_NOT_COMMIT
				sm.smLog.Printf("reply put/append op fail %v\n", applyReplyMsg)
			}
		case <-time.After(sm.timeOut):
			sm.smLog.Printf("Move time out: %v\n", op)
			reply.Err = ERR_NOT_COMMIT
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// Your code here.
	clerkId := args.ClerkId
	sm.applyReplyChMap.LoadOrStore(clerkId, make(chan ApplyReplyArgs))

	op := Op{ClerkId: args.ClerkId, SerialNum: args.SerialNum, LeaderId: sm.me, Type: queryOp, Num: args.Num}

	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	reply.Err = ""
	if !isLeader {
		reply.Err = ERR_NOT_LEADER
	} else {
		sm.smLog.Printf("receive QueryArgs: %v\n", args)
		ch, _ := sm.applyReplyChMap.Load(clerkId)
		applyReplyCh := ch.(chan ApplyReplyArgs)
		select {
		case applyReplyMsg := <-applyReplyCh:
			for applyReplyMsg.CommitIndex < index {
				sm.smLog.Printf("apply an old cmd: %v\n", applyReplyMsg)
				select {
				case applyReplyMsg = <-applyReplyCh:
				case <-time.After(sm.timeOut):
					sm.smLog.Printf("time out: %v\n", op)
					reply.Err = ERR_NOT_COMMIT
					return
				}
			}

			if index == applyReplyMsg.CommitIndex && applyReplyMsg.Command.ClerkId == op.ClerkId && applyReplyMsg.Command.SerialNum == op.SerialNum {
				sm.smLog.Printf("query reply: %v\n", applyReplyMsg.Configuration)
				reply.Config = applyReplyMsg.Configuration
			} else {
				reply.Err = ERR_NOT_COMMIT
				sm.smLog.Printf("reply put/append op fail %v\n", applyReplyMsg)
			}
		case <-time.After(sm.timeOut):
			sm.smLog.Printf("Query time out: %v\n", op)
			reply.Err = ERR_NOT_COMMIT
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.logFile.Close()
}

// needed by shardsm tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) apply() {
	for true {
		sm.smLog.Println("start to apply")
		applyMsg := <-sm.applyCh
		command := applyMsg.Command.(Op)
		sm.smLog.Printf("apply: %v\n", command)
		if sm.me == command.LeaderId {
			ch, _ := sm.applyReplyChMap.LoadOrStore(command.ClerkId, make(chan ApplyReplyArgs))
			applyReplyCh := ch.(chan ApplyReplyArgs)
			var configuration Config
			if command.Num == -1 || command.Num > len(sm.configs)-1 {
				configuration = sm.configs[len(sm.configs)-1]
			} else {
				configuration = sm.configs[command.Num]
			}
			select {
			case applyReplyCh <- ApplyReplyArgs{Command: command, CommitIndex: applyMsg.CommandIndex, Configuration: configuration}:
			default:
			}
		}

		switch command.Type {
		case joinOp:
			serialNum, _ := sm.serialNums.LoadOrStore(command.ClerkId, 0)
			if serialNum.(int) < command.SerialNum {
				sm.serialNums.Store(command.ClerkId, command.SerialNum)
				newConfig := Config{}
				newConfig.Num = len(sm.configs)
				newConfig.Groups = make(map[int][]string)
				currentConfig := sm.configs[len(sm.configs)-1]
				for k, v := range currentConfig.Groups {
					newConfig.Groups[k] = v
				}
				for i, v := range currentConfig.Shards {
					newConfig.Shards[i] = v
				}
				for k, v := range command.Servers {
					newConfig.Groups[k] = v
				}
				var gids []int
				for k, _ := range newConfig.Groups {
					gids = append(gids, k)
				}
				sort.Ints(gids)
				if len(gids) == 0 {
					for i := 0; i < NShards; i++ {
						newConfig.Shards[i] = 0
					}
				} else {
					for i := 0; i < NShards; i++ {
						newConfig.Shards[i] = gids[i%len(newConfig.Groups)]
					}
				}

				sm.configs = append(sm.configs, newConfig)
			}
		case leaveOp:
			serialNum, _ := sm.serialNums.LoadOrStore(command.ClerkId, 0)
			if serialNum.(int) < command.SerialNum {
				sm.serialNums.Store(command.ClerkId, command.SerialNum)
				newConfig := Config{}
				newConfig.Num = len(sm.configs)
				newConfig.Groups = make(map[int][]string)
				currentConfig := sm.configs[len(sm.configs)-1]
				for k, v := range currentConfig.Groups {
					newConfig.Groups[k] = v
				}
				for i, v := range currentConfig.Shards {
					newConfig.Shards[i] = v
				}

				for _, v := range command.GIDs {
					delete(newConfig.Groups, v)
				}

				var gids []int
				for k, _ := range newConfig.Groups {
					gids = append(gids, k)
				}
				sort.Ints(gids)
				if len(gids) == 0 {
					for i := 0; i < NShards; i++ {
						newConfig.Shards[i] = 0
					}
				} else {
					for i := 0; i < NShards; i++ {
						newConfig.Shards[i] = gids[i%len(newConfig.Groups)]
					}
				}

				sm.configs = append(sm.configs, newConfig)

			}
		case moveOp:
			serialNum, _ := sm.serialNums.LoadOrStore(command.ClerkId, 0)
			if serialNum.(int) < command.SerialNum {
				sm.serialNums.Store(command.ClerkId, command.SerialNum)
				newConfig := Config{}
				newConfig.Num = len(sm.configs)
				newConfig.Groups = make(map[int][]string)
				currentConfig := sm.configs[len(sm.configs)-1]
				for k, v := range currentConfig.Groups {
					newConfig.Groups[k] = v
				}
				for i, v := range currentConfig.Shards {
					newConfig.Shards[i] = v
				}
				newConfig.Shards[command.Shard] = command.GID
				sm.configs = append(sm.configs, newConfig)

			}
		case queryOp:
			serialNum, _ := sm.serialNums.LoadOrStore(command.ClerkId, 0)
			if serialNum.(int) < command.SerialNum {
				sm.serialNums.Store(command.ClerkId, command.SerialNum)

			}
		}
		sm.smLog.Printf("sm.configs: %v\n",sm.configs)

	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	//fileName := strconv.Itoa(sm.me) + ".log"
	//f, err := os.Create(fileName)
	//if err != nil {
	//	panic(err)
	//}
	//sm.logFile = f
	sm.smLog = log.New(ioutil.Discard, "[server "+strconv.Itoa(sm.me)+"] ", log.Lmicroseconds)
	//sm.smLog.Printf("servers number: %d\n", len(servers))
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.timeOut = 1000 * time.Millisecond
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.rf.RfLog.SetOutput(ioutil.Discard)

	go sm.apply()

	return sm
}
