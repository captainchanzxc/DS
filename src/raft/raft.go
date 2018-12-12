package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type nodeTye string

const (
	Candidate nodeTye = "candidate"
	Follower          = "follower"
	Leader            = "leader"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	lastHeardTime     int64               //the peer heard from leader last time
	electionTimeOut   int64
	heartBeatInterval int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VotedFor    int
	Logs        []Log

	//volatile state on all servers
	CommitIndex int
	LastApplied int
	State       nodeTye

	//volatile state on leaders
	NextIndex  []int
	MatchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.CurrentTerm
	switch rf.State {
	case Leader:
		isleader = true
	default:
		isleader = false
	}
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VoterGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.resetElectionTimeOut()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.VotedFor = args.CandidateId
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.VoterGranted = false
	} else if args.LastLogTerm > rf.CurrentTerm || (args.LastLogTerm == rf.CurrentTerm && args.LastLogIndex > len(rf.Logs)-1) {
		reply.VoterGranted = true
	} else {
		reply.VoterGranted = false

	}
	//fmt.Printf("%s peer %d current term %d vote peer %d current term %d: %t\n", time.Now().Format("2006/01/02/ 15:03:04.000"), rf.me, rf.CurrentTerm, args.CandidateId, args.Term, reply.VoterGranted)
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = Follower
	}

	return

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {

	fmt.Printf("%s peer %d starts election\n", time.Now().Format("2006/01/02/ 15:03:04.000"), rf.me)
	rf.mu.Lock()
	rf.CurrentTerm += 1
	rf.mu.Unlock()

	rf.resetElectionTimeOut()
	go rf.startElectionTimeOut()

	rva := RequestVoteArgs{}
	rva.Term = rf.CurrentTerm
	if len(rf.Logs) == 0 {
		rva.LastLogIndex = -1
		rva.LastLogTerm = rf.CurrentTerm
	} else {
		rva.LastLogIndex = len(rf.Logs) - 1
		rva.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
	}

	rva.CandidateId = rf.me
	count := 1

	//fmt.Println("start")
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {

			rvr := RequestVoteReply{}
			rf.sendRequestVote(peer, &rva, &rvr)

			rf.mu.Lock()
			if rvr.Term > rf.CurrentTerm {
				rf.CurrentTerm = rvr.Term
				rf.State = Follower
				rf.mu.Unlock()
				rf.resetElectionTimeOut()
				rf.startElectionTimeOut()
				return
			}
			rf.mu.Unlock()
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rvr.VoterGranted {
				count += 1
				if count > len(rf.peers)/2 {
					fmt.Printf("%s peer %d becomes leader\n", time.Now().Format("2006/01/02/ 15:03:04.000"), rf.me)
					rf.State = Leader
					go rf.startHeartBeat()
					//如果不加这一行，则选出的leader会启动多个心跳线程
					count = 0
				}
			}

		}(i)
	}

}

func (rf *Raft) startElectionTimeOut() {
	var timeOut int64
	var constantItv int64
	timeOut = 0
	constantItv = 10

	for true {
		time.Sleep(time.Duration(constantItv) * time.Millisecond)
		rf.mu.Lock()
		timeOut = time.Now().UnixNano()/int64(time.Millisecond) - rf.lastHeardTime
		if timeOut >= rf.electionTimeOut {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	//只有Follower和Candidate在election time out后才会发起投票，由于本程序的原因，在candidate选举时也会启动
	//一个electionTimeOut线程，其在成为Leader不应该再次进行投票了
	if rf.State == Follower || rf.State == Candidate {
		rf.mu.Unlock()
		rf.State = Candidate
		rf.startElection()
		return
	}
	rf.mu.Unlock()

}
func (rf *Raft) resetElectionTimeOut() {
	rand.Seed(time.Now().UnixNano())
	rf.mu.Lock()
	rf.lastHeardTime = time.Now().UnixNano() / int64(time.Millisecond)
	rf.electionTimeOut = rand.Int63n(150) + 350
	rf.mu.Unlock()
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.resetElectionTimeOut()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	if args.PrevLogIndex==-1{
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.State = Follower
		}
		fmt.Printf("%s leader %d current term %d append peer %d current term %d\n",time.Now().Format("2006/01/02/ 15:03:04.000"),args.LeaderId,args.Term,rf.me,rf.CurrentTerm)
		reply.Success=true
		return
	}

	if args.Term < rf.CurrentTerm ||rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.State = Follower
		}
		return
	}
	//以startPosition为起点,dist是插入点appendPosition到startPosition的距离
	startPosition := args.PrevLogIndex + 1
	dist := 0
	for ; dist < len(args.Entries) && (startPosition+dist) < len(rf.Logs); dist++ {
		if rf.Logs[startPosition+dist].Term != args.Entries[dist].Term {
			rf.Logs = rf.Logs[:startPosition+dist-1]
			break
		}
	}
	rf.Logs = append(rf.Logs, args.Entries[dist+1:]...)

	//todo
	if args.LeaderCommit > rf.CommitIndex {
		if args.LeaderCommit < args.PrevLogIndex+dist+1 {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = args.PrevLogIndex + dist + 1
		}
	}

}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startHeartBeat() {
	rf.heartBeatInterval = 100
	for true {
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}
			go func(peer int) {
				reply := AppendEntryReply{}
				args := AppendEntryArgs{}
				args.LeaderId = rf.me
				rf.mu.Lock()
				args.Term = rf.CurrentTerm
				args.PrevLogIndex=-1
				args.PrevLogTerm=-1
				rf.sendAppendEntries(peer, &args, &reply)
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.State = Follower
					rf.mu.Unlock()
					rf.resetElectionTimeOut()
					rf.startElectionTimeOut()
					return
				}
				rf.mu.Unlock()

			}(i)
		}
		time.Sleep(time.Duration(rf.heartBeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) replicateLog() {
	rf.mu.Lock()
	peersNum := len(rf.peers)
	rf.mu.Unlock()
	for i := 0; i < peersNum; i++ {
			go func(peer int) {
				rf.mu.Lock()
				nextIndex := rf.NextIndex[i]
				isLarge := len(rf.Logs)-1 >= nextIndex
				if !isLarge{
					rf.mu.Unlock()
					return
				}
				args := AppendEntryArgs{}
				reply := AppendEntryReply{}
				args.Term = rf.CurrentTerm
				args.PrevLogIndex=rf.NextIndex[i]-1
				args.PrevLogTerm=rf.Logs[args.PrevLogIndex].Term
				args.LeaderId = rf.me
				entries:=rf.Logs[nextIndex:nextIndex+1]
				args.Entries = append(args.Entries, entries...)
				rf.mu.Unlock()
				rf.sendAppendEntries(i, &args, &reply)
				//每个线程只更改自己对应的nextIndex和matchIndex，因此这里不用做并发控制
				if reply.Success {
					rf.NextIndex[i] += len(entries)
					rf.MatchIndex[i] += len(entries) - 1
				} else {
					rf.NextIndex[i] -= 1
				}
			}(i)
		//todo
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	index = len(rf.Logs)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Logs = append(rf.Logs, Log{Term: rf.CurrentTerm, Command: command})
	term = rf.CurrentTerm
	if rf.State != Leader {
		isLeader = false
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.resetElectionTimeOut()
	rf.State = Follower
	go rf.startElectionTimeOut()
	fmt.Printf("last heard time %d, timeout %d\n", rf.lastHeardTime, rf.electionTimeOut)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
