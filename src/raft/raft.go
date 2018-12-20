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
	"bytes"
	"fmt"
	"labgob"
	"log"
	"math/rand"
	"os"
	"strconv"
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
	applyCh           chan ApplyMsg

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

	//use to debug
	rfLog   *log.Logger
	isAlive bool
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	//fmt.Printf("peer %d persist:" +
	//	"logs: %v\n" +
	//	"voted for: %d\n" +
	//	"current term: %d\n\n",
	//	rf.me,rf.Logs,rf.VotedFor,rf.CurrentTerm)

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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		fmt.Println("decode error!")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Logs = logs
	}
	//fmt.Printf("peer %d read persist:" +
	//	"logs: %v\n" +
	//	"voted for: %d\n" +
	//	"current term: %d\n\n",
	//	rf.me,rf.Logs,rf.VotedFor,rf.CurrentTerm)

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
	ReceivedTerm int
	VoterGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.ReceivedTerm = args.Term
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.VoterGranted = false
	}
	if args.Term == rf.CurrentTerm {
		//在follower的同一个term中，给定candidate的term，最多只能为一个candidate投票
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			if args.LastLogTerm > rf.Logs[len(rf.Logs)-1].Term || (args.LastLogTerm == rf.Logs[len(rf.Logs)-1].Term && args.LastLogIndex >= len(rf.Logs)-1) {
				reply.VoterGranted = true
				rf.VotedFor = args.CandidateId
			}
		}
	}
	if args.Term > rf.CurrentTerm {
		if args.LastLogTerm > rf.Logs[len(rf.Logs)-1].Term || (args.LastLogTerm == rf.Logs[len(rf.Logs)-1].Term && args.LastLogIndex >= len(rf.Logs)-1) {
			reply.VoterGranted = true
			rf.VotedFor = args.CandidateId
		}
		rf.CurrentTerm = args.Term
		rf.State = Follower
	}
	rf.persist()
	//fmt.Printf("%s peer %d(votedFor %d) current term %d vote peer %d current term %d: %t\n", time.Now().Format("2006/01/02/ 15:03:04.000"), rf.me, rf.VotedFor, reply.Term, args.CandidateId, args.Term, reply.VoterGranted)
	rf.mu.Unlock()
	if reply.VoterGranted {
		rf.resetElectionTimeOut()
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
	//	fmt.Printf("%s peer %d starts election\n", time.Now().Format("2006/01/02/ 15:03:04.000"), rf.me)
	rf.mu.Lock()
	rf.CurrentTerm += 1
	rf.persist()
	rf.VotedFor = rf.me
	rva := RequestVoteArgs{}
	rva.Term = rf.CurrentTerm
	rva.LastLogIndex = len(rf.Logs) - 1
	rva.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
	rva.CandidateId = rf.me
	rf.mu.Unlock()

	rf.resetElectionTimeOut()
	go rf.startElectionTimeOut()

	//计算得票数
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
				rf.persist()
				rf.mu.Unlock()
				rf.resetElectionTimeOut()
				rf.startElectionTimeOut()
				return
			}
			rf.mu.Unlock()
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rvr.VoterGranted && rvr.ReceivedTerm == rf.CurrentTerm {
				count += 1
				if count > len(rf.peers)/2 {
					rf.rfLog.Println("becomes leader")
					//fmt.Printf("%s peer %d becomes leader\n", time.Now().Format("2006/01/02/ 15:03:04.000"), rf.me)
					rf.State = Leader
					//这个地方当初没写也允许通过了TestBasicAgree2B 和 TestFailAgree2B，值得思考
					for j := 0; j < len(rf.NextIndex); j++ {
						rf.MatchIndex[j] = 0
						rf.NextIndex[j] = len(rf.Logs)
					}
					go rf.startHeartBeat()
					go rf.checkLeaderCommitIndex()
					go rf.replicateLog()
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
		rf.State = Candidate
		rf.mu.Unlock()
		rf.startElection()
		return
	}
	rf.mu.Unlock()

}
func (rf *Raft) resetElectionTimeOut() {

	rand.Seed(time.Now().UnixNano())
	//fmt.Printf("peer %d reset timeout1\n",rf.me)
	rf.mu.Lock()
	rf.lastHeardTime = time.Now().UnixNano() / int64(time.Millisecond)
	rf.electionTimeOut = rand.Int63n(200) + 350
	rf.mu.Unlock()
	//fmt.Printf("peer %d reset timeout2\n",rf.me)
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
	Term         int
	ReceivedTerm int
	Success      bool
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//rf.printState()
	//	fmt.Printf("before: peer %d logs: %v\n",rf.me,rf.Logs)
	if args.Term >= rf.CurrentTerm {
		go rf.resetElectionTimeOut()
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = Follower
		rf.persist()
	}
	reply.ReceivedTerm = args.Term
	reply.Term = rf.CurrentTerm
	//	fmt.Printf("%s leader %d(prev Index: %d, current Term: %d) send peer %d(current term: %d): %v\n ", time.Now().Format("2006/01/02/ 15:03:04.000"), args.LeaderId, args.PrevLogIndex,args.Term,rf.me, rf.CurrentTerm,args.Entries)
	//args.PrevLogIndex>len(rf.Logs)-1这种情况也算不match
	if args.Term < rf.CurrentTerm || args.PrevLogIndex > len(rf.Logs)-1 || rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	if rf.State == Candidate {
		rf.State = Follower
	}
	reply.Success = true
	//以startPosition为起点,dist是插入点appendPosition到startPosition的距离
	startPosition := args.PrevLogIndex + 1
	dist := 0
	for ; dist < len(args.Entries) && (startPosition+dist) < len(rf.Logs); dist++ {
		if rf.Logs[startPosition+dist].Term != args.Entries[dist].Term {
			rf.Logs = rf.Logs[:startPosition+dist]
			break
		}
	}
	if len(args.Entries) == 0 {
		//	fmt.Printf("%s leader %d send hb to peer %d\n ", time.Now().Format("2006/01/02/ 15:03:04.000"), args.LeaderId, rf.me)
	} else {
		//	fmt.Printf("%s leader %d replicate peer %d: %v\n ", time.Now().Format("2006/01/02/ 15:03:04.000"), args.LeaderId, rf.me, args.Entries[dist:])
	}
	rf.Logs = append(rf.Logs, args.Entries[dist:]...)
	rf.persist()
	//fmt.Printf("after: peer %d logs: %v\n",rf.me,rf.Logs)
	//fmt.Printf("before: leaderCommit: %d, peer %d Commit: %d, applied:%d\n",args.LeaderCommit,rf.me,rf.CommitIndex,rf.LastApplied)
	if args.LeaderCommit > rf.CommitIndex {
		if args.LeaderCommit < args.PrevLogIndex+dist {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = args.PrevLogIndex + dist
		}
	}
	//fmt.Printf("dist : %d, prev index: %d\n",dist,args.PrevLogIndex)
	//fmt.Printf("after: leaderCommit: %d, peer %d Commit: %d, applied: %d\n",args.LeaderCommit,rf.me,rf.CommitIndex,rf.LastApplied)

}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startHeartBeat() {
	//fmt.Println("start hb")
	rf.heartBeatInterval = 300
	for true {
		for i := 0; i < len(rf.peers); i++ {
			rf.mu.Lock()
			if rf.State != Leader || rf.isAlive == false {
				//fmt.Printf("leader %d return !!!\n", rf.me)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if i != rf.me {
				go func(peer int) {
					reply := AppendEntryReply{}
					args := AppendEntryArgs{}
					rf.mu.Lock()
					args.LeaderId = rf.me
					args.Term = rf.CurrentTerm
					args.PrevLogIndex = rf.NextIndex[peer] - 1
					args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
					args.LeaderCommit = rf.CommitIndex
					rf.mu.Unlock()

					rf.sendAppendEntries(peer, &args, &reply)
					rf.mu.Lock()
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						//		fmt.Printf("leader %d becomes follower\n", rf.me)
						rf.State = Follower
						rf.persist()
						rf.mu.Unlock()
						rf.resetElectionTimeOut()
						rf.startElectionTimeOut()
						return
					}
					rf.mu.Unlock()

				}(i)

			}

		}
		time.Sleep(time.Duration(rf.heartBeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) replicateLog() {
	for true {
		rf.mu.Lock()
		peersNum := len(rf.peers)
		rf.mu.Unlock()
		for i := 0; i < peersNum; i++ {

			go func(peer int) {
				rf.mu.Lock()
				nextIndex := rf.NextIndex[peer]
				isLarge := len(rf.Logs)-1 >= nextIndex
				//fmt.Printf("%s peer %d in leader %d(last log index %d) nextIndex %d\n",time.Now().Format("2006/01/02/ 15:03:04.000"),peer,rf.me,len(rf.Logs)-1,nextIndex)
				if !isLarge || rf.State != Leader {
					rf.mu.Unlock()
					return
				}
				//	rf.printState()
				args := AppendEntryArgs{}
				reply := AppendEntryReply{}
				args.Term = rf.CurrentTerm
				args.PrevLogIndex = nextIndex - 1
				args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term

				args.LeaderCommit = rf.CommitIndex
				args.LeaderId = rf.me
				entries := rf.Logs[nextIndex : nextIndex+1]
				args.Entries = append(args.Entries, entries...)
				//		fmt.Printf("leader %d append to peer %d: %v\n", rf.me, peer, entries)
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(peer, &args, &reply)
				if ok {
					rf.mu.Lock()
					if reply.ReceivedTerm != rf.CurrentTerm {
						rf.mu.Unlock()
						return
					}
					//fmt.Printf("peer %d %t!!!!!!!\n",peer,reply.Success)
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						//	fmt.Printf("leader %d becomes follower in replicate\n", rf.me)
						rf.State = Follower
						rf.persist()
						rf.mu.Unlock()
						rf.resetElectionTimeOut()
						rf.startElectionTimeOut()
						return
					}
					if reply.Success {
						rf.NextIndex[peer] = nextIndex + len(entries)
						rf.MatchIndex[peer] = args.PrevLogIndex + len(entries)
					} else {
						rf.NextIndex[peer] = nextIndex - 1
					}
					//	fmt.Printf("match index: %v\n", rf.MatchIndex)
					//	fmt.Printf("next index: %v\n", rf.NextIndex)
					rf.mu.Unlock()
				}

			}(i)
			rf.mu.Lock()
			if rf.State != Leader || rf.isAlive == false {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}

		time.Sleep(time.Duration(30) * time.Millisecond)
	}

}

func (rf *Raft) checkApplied() {
	for true {

		rf.mu.Lock()
		if rf.isAlive == false {
			rf.mu.Unlock()
			break
		}
		//		fmt.Printf("%s peer %d commitIndex: %d, appliedIndex: %d\n", time.Now().Format("2006/01/02/ 15:03:04.000"), rf.me, rf.CommitIndex, rf.LastApplied)
		if rf.CommitIndex > rf.LastApplied {
			rf.LastApplied += 1
			applyMsg := ApplyMsg{Command: rf.Logs[rf.LastApplied].Command, CommandIndex: rf.LastApplied, CommandValid: true}
			//fmt.Printf("peer %d applied: %v\n", rf.me, rf.Logs[rf.LastApplied].Command)
			rf.applyCh <- applyMsg
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) checkLeaderCommitIndex() {
	for true {
		rf.mu.Lock()
		if rf.State != Leader || rf.isAlive == false {
			rf.mu.Unlock()
			break
		}
		for N := rf.CommitIndex + 1; N < len(rf.Logs); N++ {
			count := 0
			if rf.Logs[N].Term == rf.CurrentTerm {

				for p := 0; p < len(rf.MatchIndex); p++ {
					if rf.MatchIndex[p] >= N {
						count++
					}
				}
				if count > len(rf.MatchIndex)/2 {
					rf.CommitIndex = N
					break
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != Leader {
		isLeader = false
	} else {
		index = len(rf.Logs)

		rf.Logs = append(rf.Logs, Log{Term: rf.CurrentTerm, Command: command})
		rf.persist()
		term = rf.CurrentTerm
		//	fmt.Println(rf.Logs)
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
	rf.mu.Lock()
	rf.isAlive = false
	rf.mu.Unlock()
}

func (rf *Raft) printState() {
	fmt.Printf(">>>>>>>>peer %d state start print\n"+
		"state: %s\n"+
		"current term: %d\n"+
		"voted for: %d\n"+
		"commit inex: %d\n"+
		"last applied: %d\n"+
		"logs: %v\n"+
		"next index: %v\n"+
		"match index: %v\n"+
		"<<<<<<<<<peer %d state end print\n\n",

		rf.me,
		rf.State,
		rf.CurrentTerm,
		rf.VotedFor,
		rf.CommitIndex,
		rf.LastApplied,
		rf.Logs,
		rf.NextIndex,
		rf.MatchIndex,
		rf.me)
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
	rf.rfLog = log.New(os.Stdout, "[raft "+strconv.Itoa(rf.me)+"] ", log.Lmicroseconds)
	rf.CurrentTerm = 0
	rf.resetElectionTimeOut()
	rf.State = Follower
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.VotedFor = -1
	rf.isAlive = true
	rf.applyCh = applyCh
	rf.Logs = append(rf.Logs, Log{0, 0})
	for j := 0; j < len(rf.peers); j++ {
		rf.NextIndex = append(rf.NextIndex, len(rf.Logs))
		rf.MatchIndex = append(rf.MatchIndex, 0)
	}
	//rf.persist()
	go rf.startElectionTimeOut()
	go rf.checkApplied()
	//fmt.Printf("last heard time %d, timeout %d\n", rf.lastHeardTime, rf.electionTimeOut)
	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	return rf
}
