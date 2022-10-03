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
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

var startTime time.Time

func init() {
	log.SetFlags(0)
	startTime = time.Now()
}

func timestamp() int64 {
	return time.Since(startTime).Milliseconds()
}

func rlog(format string, v ...interface{}) {
	// log.SetPrefix(fmt.Sprint(timestamp(), " => "))
	// log.Printf(format, v...)
}

func rand_heartbeat(me int) int64 {
	new_heartbeat := MinHeartbeatTimeout + rand.Int63n(MaxHeartbeatTimeout-MinHeartbeatTimeout)
	rlog("[%d] Generated new heartbeat timeout %d", me, new_heartbeat/1_000_000)
	return new_heartbeat
}

const (
	Follower int32 = iota
	Leader
	Candidate
)

const (
	HeartbeatInterval = 100 * time.Millisecond // how often to send out hearbeats as a leader

	MaxHeartbeatTimeout = int64(600 * time.Millisecond) // max timeout to wait for a hearbeat from the leader
	MinHeartbeatTimeout = int64(200 * time.Millisecond)
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm         int   // the current term
	leaderFollowerState int32 // are we leader
	votedFor            int   // who did we vote for in the current term

	lastHeartbeat    time.Time     // when did we las receive a heatbeat
	heartbeatTimeout time.Duration // how long to wait for hearbeat (in milliseconds)

	votesReceived int // how many positive votes we received as a candidate
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.leaderFollowerState == Leader
	rf.mu.Unlock()

	return
}

func (rf *Raft) getState() (me int, term int, leaderFollower int32) {
	rf.mu.Lock()

	me = rf.me
	term = rf.currentTerm
	leaderFollower = rf.leaderFollowerState

	rf.mu.Unlock()
	return
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int // who's asking for the vote
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// todo: examine index and term of log

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rlog("[%d] currentTerm=%d is requested to vote for [%d] term=%d",
		rf.me, rf.currentTerm, args.CandidateId, args.Term)

	if args.Term > rf.currentTerm {
		rlog("[%d] is now following [%d] because its term is greater", rf.me, args.CandidateId)
		rf.leaderFollowerState = Follower
		// update our term
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.heartbeatTimeout = time.Duration(rand_heartbeat(rf.me))
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = (rf.votedFor == args.CandidateId)
		}
	}

	// Question: should we set lastHeartbeat=now() when we grant a positive vote so
	// we don't start a new election soon after we voted?
	// Answer: I guess so, [1] starts election, [2] votes for [1] and its old hearbeat expires
	// so [2] starts a new election with a higher term than [1], [3] votes for [2] and its old
	// hearbeat expires so [3] starts a new election with a higher term than [2], etc.
	if reply.VoteGranted {
		rf.lastHeartbeat = time.Now()
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
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

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() {

		_, _, leaderFollowerState := rf.getState()
		if leaderFollowerState == Leader {
			go rf.sendHeartbeat()
		}
		time.Sleep(HeartbeatInterval)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	rf.mu.Lock()
	rf.heartbeatTimeout = time.Duration(rand_heartbeat(rf.me))
	sleep_for := rf.heartbeatTimeout
	rf.mu.Unlock()

	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(sleep_for)

		rf.mu.Lock()

		if time.Since(rf.lastHeartbeat) < rf.heartbeatTimeout {
			// we received a heartbeat in the meantime so sleep some more
			sleep_for = time.Until(rf.lastHeartbeat.Add(rf.heartbeatTimeout))
		} else
		// heartbeat has expired
		if rf.leaderFollowerState == Follower {
			// become candidate
			rf.leaderFollowerState = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.votesReceived = 1
			rlog("[%d] currentTerm=%d has become candidate\n", rf.me, rf.currentTerm)

			rf.lastHeartbeat = time.Now()
			rf.heartbeatTimeout = time.Duration(rand_heartbeat(rf.me))
			sleep_for = rf.heartbeatTimeout

			for i := range rf.peers {
				if i != rf.me {
					go rf.sendRequestVoteToPeer(i)
				}
			}
		} else if rf.leaderFollowerState == Candidate {
			// restart the election
			rf.currentTerm++
			rlog("[%d] restarting election with term=%d", rf.me, rf.currentTerm)

			for i := range rf.peers {
				if i != rf.me {
					go rf.sendRequestVoteToPeer(i)
				}
			}
			rf.lastHeartbeat = time.Now()
			sleep_for = rf.heartbeatTimeout
		} else if rf.leaderFollowerState == Leader {
			sleep_for = rf.heartbeatTimeout
		}

		rf.mu.Unlock()
	}
}

// this method SHOULD ONLY be called as a goroutine
func (rf *Raft) sendRequestVoteToPeer(i int) {

	var reply RequestVoteReply

	for {
		if rf.killed() {
			return
		}

		me, term, leaderFollowerState := rf.getState()

		if leaderFollowerState != Candidate {
			return
		}

		rlog("[%d] currentTerm=%d sending requestVote to [%d]", me, term, i)
		reply = RequestVoteReply{}

		args := RequestVoteArgs{
			CandidateId: me,
			Term:        term,
		}

		ok := rf.sendRequestVote(i, &args, &reply)
		if ok && reply.Term < term {
			rlog("[%d] retrying to send RequestVote because reply.Term %d is less than %d",
				rf.me, reply.Term, term)
			// the reply from peer came very very late (peer was in network partition? or failed?)
			continue
		}
		if ok {
			break
		}

		// if !ok, we repeat the for loop and send the request again
	}

	// check that we're still candidates, we could have become followers in the meantime
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leaderFollowerState != Candidate {
		return
	}

	if rf.killed() {
		return
	}

	rlog("[%d] currentTerm=%d RequestVote reply received from [%d] term=%d\n with Granted=%v", rf.me, rf.currentTerm, i, reply.Term, reply.VoteGranted)

	if reply.Term > rf.currentTerm {
		rf.leaderFollowerState = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.heartbeatTimeout = time.Duration(rand_heartbeat(rf.me))
		rf.lastHeartbeat = time.Now()
		return
	}

	if reply.VoteGranted {
		rf.votesReceived++

		if rf.votesReceived > len(rf.peers)/2 {
			rlog("[%d] currentTerm=[%d] is becoming leader", rf.me, rf.currentTerm)
			rf.leaderFollowerState = Leader
			rf.heartbeatTimeout = HeartbeatInterval
		}
	}

}

// this method SHOULD ONLY be called as a goroutine
func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeartbeat = time.Now()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.sendHeartbeatToPeer(i)
	}
}

// this method SHOULD ONLY be called as a goroutine
func (rf *Raft) sendHeartbeatToPeer(i int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	rf.peers[i].Call("Raft.AppendEntries", &args, &reply)

}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rlog("[%d] currentTerm=%d is switching to Follower, because leader [%d] term=%d",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.currentTerm = args.Term
		rf.leaderFollowerState = Follower
		rf.heartbeatTimeout = time.Duration(rand_heartbeat(rf.me))
		rf.votedFor = -1
		rf.votesReceived = 0
	}

	if rf.leaderFollowerState == Leader {
		rlog("[WARNING] [%d] we are leader currentTerm=[%d] but another leader [%d] term=[%d] has sent us AppendEntries\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
		return
	}

	if rf.leaderFollowerState == Candidate && rf.currentTerm <= args.Term {
		rlog("[%d] currentTerm=%d Candidate, has received AppendEntries from [%d] term=%d, switching to Follower",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.leaderFollowerState = Follower
		rf.heartbeatTimeout = time.Duration(rand_heartbeat(rf.me))
	}

	rf.lastHeartbeat = time.Now()

	reply.Term = rf.currentTerm
	rlog("[%d] received heartbeat from [%d]", rf.me, args.LeaderId)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderFollowerState = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.lastHeartbeat = time.Now()
	go rf.ticker()
	go rf.leaderTicker()

	return rf
}
