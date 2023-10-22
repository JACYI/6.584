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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type Role int

const (
	Follower = iota + 1
	Candidate
	Leader
)

var base = 3
var ExpiredTime = 150 * base
var HeartBeatsGap = 10 * base

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index int
	Term  int
	Value interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     Role

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int // latest term number
	VoteFor     int // candidateId of current server voted for in this term
	Voted       bool

	HeartBeatsTicker  *time.Ticker
	RequestVoteTicker *time.Ticker

	//Log []LogEntry // Log Entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // number of new term
	CandidateId  int // the ID of vote requester
	LastLogIndex int // the index of candidate's last log entry
	LastLogTerm  int // the term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // to refuse the vote and reply candicate to update its log to Term
	VoteGranted bool // is candidate get the vote
	Voted       int  // the id of current server granted in this "Term"
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// request for old term and return false
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		DPrintf("server[%v] drop vote request of server[%v] with term [%v]", rf.me, args.CandidateId, args.Term)
		return
	}
	// TODO: if request last log entry index is less than receiver's, cancel the vote

	// new term election
	if args.Term > rf.CurrentTerm {
		rf.VoteFor = args.CandidateId
		rf.CurrentTerm = args.Term
		rf.state = Follower
		rf.HeartBeatsTicker.Stop()
		rf.persist()
		DPrintf("server[%v:%v] votes for server[%v]\n", rf.me, rf.CurrentTerm, args.CandidateId)

		reply.VoteGranted = true
		reply.Voted = args.CandidateId
		reply.Term = args.Term
		return
	}

	// request voted term equals to current term
	if rf.VoteFor == args.CandidateId {
		// voted for request candidate before
		reply.VoteGranted = true
	} else {
		// current server has voted for other candidate
		reply.VoteGranted = false
		reply.Voted = rf.VoteFor
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, votes *atomic.Int32, waits *sync.WaitGroup) {
	defer waits.Done()
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)

	if !ok {
		DPrintf("failed to send rpc[vote] to server[%v]\n", server)
		return
	}
	DPrintf("server[%v] receive vote result from server[%v], voted:[%v],Term[%v]",
		rf.me, server, reply.VoteGranted, reply.Term)

	rf.mu.Lock()
	if reply.Term < rf.CurrentTerm {
		// drop the expired response

	} else if reply.Term > rf.CurrentTerm {
		// the real leader has been elected, current server returns to follower state
		rf.state = Follower
		rf.HeartBeatsTicker.Stop()
		rf.ResetVoteTicker()
	} else if reply.VoteGranted && reply.Voted == rf.me {
		votes.Add(1)

		DPrintf("server[%v] get votes[%v/%v] at term[%v]", rf.me, votes.Load(), len(rf.peers), args.Term)
		if rf.state == Candidate && rf.CurrentTerm == args.Term && int(votes.Load()) > (len(rf.peers)/2) {
			rf.WinTheElection()
		}
	}

	rf.mu.Unlock()
}

// AppendEntries rpc call is used to send heartbeat to others and log republication
type AppendEntriesArgs struct {
	Term        int
	LeaderId    int
	IsHeartBeat bool
}
type AppendEntriesReply struct {
	Ok bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Ok = true
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		if rf.state == Leader {
			rf.HeartBeatsTicker.Stop()
		}
		rf.state = Follower
	}
	if args.Term == rf.CurrentTerm {
		// TODO: append the entries

		if args.IsHeartBeat {
			DPrintf("server[%v] receive the heartbeats from server[%v] at term[%v]", rf.me, args.LeaderId, args.Term)
			rf.ResetVoteTicker()
		}
	} else {
		// expired heartbeats
		reply.Ok = false
	}

}

func (rf *Raft) sendAppendEntries(server int, resp *atomic.Int32, args *AppendEntriesArgs) error {

	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		DPrintf("failed to send rpc[appe] from server[%v] to server[%v]", rf.me, server)
		return fmt.Errorf("failed to send append entries request from server[%v] to server[%v]\n", rf.me, server)
	}

	//if reply.Ok {
	//	resp.Add(1)
	//}
	// TODO: supply the handle process of reply

	return nil
}

func (rf *Raft) Election() {
	rf.mu.Lock()
	// Check if a leader election should be started.
	term, isLeader := rf.GetState()
	if isLeader {
		rf.mu.Unlock()
		return
	}

	// update to candidate
	rf.state = Candidate
	rf.CurrentTerm = term + 1
	rf.VoteFor = rf.me
	args := RequestVoteArgs{
		Term:        rf.CurrentTerm,
		CandidateId: rf.me,
		// TODO: supply the other items
	}
	DPrintf("[%v] start election at term[%v]", rf.me, rf.CurrentTerm)
	// init the wait group for voted
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(rf.peers) - 1)
	votes := atomic.Int32{}
	votes.Store(1)
	// rpc call can not be permitted to obtain the lock
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args, &votes, &waitGroup)
	}

	waitGroup.Wait()
}

func (rf *Raft) WinTheElection() {
	DPrintf("[~WINNER~]: server[%v]@t%v win the election", rf.me, rf.CurrentTerm)
	rf.state = Leader
	rf.Voted = false
	rf.VoteFor = -1

	rf.persist()

	rf.SendHeartBeats()
	rf.ResetHeartBeatsTicker()
}

func (rf *Raft) SendHeartBeats() {

	args := AppendEntriesArgs{
		Term:        rf.CurrentTerm,
		LeaderId:    rf.me,
		IsHeartBeat: true,
	}

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		//replys := atomic.Int32{}
		//replys.Add(1)
		//err := rf.sendAppendEntries(i, &replys, &args)
		err := rf.sendAppendEntries(i, nil, &args)
		if err != nil {
			DPrintf("failed to call append entries rpc, %v", err)
		}
	}
}

type rpcCallRes struct {
	server  int
	success bool
	reply   *RequestVoteReply
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		select {
		case <-rf.RequestVoteTicker.C:
			// request vote time out
			// Check if a leader election should be started.
			rf.Election()
		case <-rf.HeartBeatsTicker.C:
			rf.mu.Lock()
			_, isLeader := rf.GetState()
			if isLeader {
				rf.SendHeartBeats()
			}
			rf.mu.Unlock()
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:                sync.Mutex{},
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              0,
		state:             Follower,
		CurrentTerm:       0,
		VoteFor:           -1,
		Voted:             false,
		HeartBeatsTicker:  time.NewTicker(time.Duration(HeartBeatsGap) * time.Millisecond),
		RequestVoteTicker: time.NewTicker(time.Duration(ExpiredTime) * time.Millisecond),
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.HeartBeatsTicker.Stop()
	rf.ResetVoteTicker()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// ticker functions
func (r *Raft) ResetVoteTicker() {
	// reset for 1500~3000 ms
	mseconds := ExpiredTime + rand.Intn(ExpiredTime)
	r.RequestVoteTicker.Reset(time.Millisecond * time.Duration(mseconds))
}

func (r *Raft) ResetHeartBeatsTicker() {
	r.HeartBeatsTicker.Reset(time.Millisecond * time.Duration(HeartBeatsGap))

}
