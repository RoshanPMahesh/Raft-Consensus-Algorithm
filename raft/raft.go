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

// Bug 0: Switches to follower upon receiving one false vote
// Bug 1: Servers commit without majority when not leader

import "sync"
import "sync/atomic"
import "raft/labrpc"
import "time"
import "math/rand"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntries struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.RWMutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// You may also need to add other state, as per your implementation.
	currentTerm int          // latest term server has seen - initialized to 0 on boot
	votedFor    int          // candidateId that received vote in current term, or null/-1 if none
	log         []LogEntries // log entries - each entry contains command and term (first index is 1)

	commitIndex int // index of highest log entry known to be committed - initialized to 0
	lastApplied int // index of highest log entry applied to state machine - initialized to 0

	nextIndex  []int // for each server, index of the next log entry to send to that server - initialized to leader last log index + 1
	matchIndex []int // for each server, index of highest log entry known to be replicated on server - initialized to 0

	// other states
	serverState   string    // either follower, candidate, or leader
	numVotes      int       // number of votes a server has received
	lastHeartBeat time.Time // time of last heartbeat
	voteTimeOut   time.Time // time of last vote

	applyCh chan ApplyMsg
	successNum int 	// num of success appending entries
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int          // leader's term
	LeaderId     int          // so follower can redirect clients
	PrevLogIndex int          // index of log entry immediately preceding NEW ones
	PrevLogTerm  int          // term of PrevLogIndex entry
	Entries      []LogEntries // log entries to store - empty for heartbeats
	LeaderCommit int          // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	Behind bool // true if behind
	CurrIndex int  // holds the index that the receiving server is at (1-indexed)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.serverState == "Leader" {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) checkUpToDate(argsLastLogTerm int, argsLastLogIndex int) bool {
	var receiveServerIndex int
	var receiveServerTerm int
	if len(rf.log) > 0 {
		receiveServerIndex = len(rf.log)
		receiveServerTerm = rf.log[len(rf.log)-1].Term
	} else {
		receiveServerIndex = 0
		receiveServerTerm = 0
	}
	//receiveServerIndex = receiveServerIndex + 0

	if argsLastLogTerm > receiveServerTerm || (argsLastLogTerm == receiveServerTerm && argsLastLogIndex >= receiveServerIndex) {
		return true
	} else {
		return false
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".

	rf.mu.Lock()
	defer rf.mu.Unlock() // it decides when to unlock
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		//fmt.Println("RECEIVER: ", rf.currentTerm)
		//fmt.Println("SENDER: ", args.Term)
		rf.currentTerm = args.Term
		//fmt.Println("REQUESTVOTE TERM: ", rf.currentTerm)
		rf.votedFor = -1            // null
		rf.serverState = "Follower" // another server has higher term, so make sure it's a follower
		rf.numVotes = 0
	}
	//fmt.Println("TERM: ", args.LastLogTerm)
	//fmt.Println("INDEX: ", args.LastLogIndex)

	// i think we need to make sure that the it's the most up to date log too??
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.checkUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		rf.voteTimeOut = time.Now()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok == true {
		rf.mu.Lock()
		if reply.VoteGranted == true {
			rf.numVotes = rf.numVotes + 1 // received vote
		} else if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			//fmt.Println("SENDREQUESTVOTE: ", rf.currentTerm)
			rf.serverState = "Follower" // another server had higher term, so revert this back to follower

		}
		rf.mu.Unlock()
	}

	return ok

}

func (rf *Raft) ReceiveMessage(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Success = false
		reply.Behind = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Behind = false
		return
	} else if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.serverState = "Follower"
		rf.mu.Unlock()
	}

	go rf.heartTimeOut(300)
	rf.lastHeartBeat = time.Now()

	if len(rf.log) < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Behind = true
		reply.CurrIndex = len(rf.log)
		return
	}
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {	// this case might have to do with fixing old logs
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Behind = true
		reply.CurrIndex = args.PrevLogIndex - 1
		return
	}

	if args.PrevLogIndex+1 <= len(rf.log) {
		index := args.PrevLogIndex+1
		for i := 0; i < len(args.Entries); i++ {
			if index <= len(rf.log) {
				rf.mu.Lock()
				rf.log[index-1] = args.Entries[i]
				rf.mu.Unlock()
				index++
			} else {
				rf.mu.Lock()
				rf.log = append(rf.log, args.Entries[i])
				rf.mu.Unlock()
				index++
			}
		}
	} else {
		for i := 0; i < len(args.Entries); i++ {
			rf.mu.Lock()
			rf.log = append(rf.log, args.Entries[i])
			rf.mu.Unlock()
		}
	}
	
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log) {
			for i := rf.commitIndex; i < len(rf.log); i++ {
				var applied ApplyMsg
				applied.CommandValid = true
				rf.mu.RLock()
				applied.Command = rf.log[i].Command
				rf.mu.RUnlock()
				applied.CommandIndex = i+1
				rf.applyCh <- applied
				rf.lastApplied++
			}
			rf.mu.Lock()
			rf.commitIndex = len(rf.log)
			rf.mu.Unlock()
		} else {
			for i := rf.commitIndex; i < args.LeaderCommit; i++ {
				var applied ApplyMsg
				applied.CommandValid = true
				rf.mu.RLock()
				applied.Command = rf.log[i].Command
				rf.mu.RUnlock()
				applied.CommandIndex = i+1
				rf.applyCh <- applied
				rf.lastApplied++
			}

			rf.mu.Lock()
			rf.commitIndex = args.LeaderCommit
			rf.mu.Unlock()
		}
	}
		
	reply.Behind = false
	reply.CurrIndex = len(rf.log)
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	ok := rf.peers[server].Call("Raft.ReceiveMessage", args, reply)
	if ok == true {
		if reply.Success == true {
			rf.mu.Lock()
			rf.successNum++
			rf.matchIndex[server] = rf.nextIndex[server]
			rf.nextIndex[server]++
			rf.mu.Unlock()
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.serverState = "Follower"
			}
			if reply.Behind == true {
				if reply.CurrIndex == len(rf.log) {
					args.Entries = []LogEntries{}
				}
				for i := len(rf.log) - 1; i >= reply.CurrIndex; i-- {
					args.Entries = append([]LogEntries{rf.log[i]}, args.Entries...)
				}

				args.PrevLogIndex = reply.CurrIndex
				args.PrevLogTerm = rf.log[reply.CurrIndex-1].Term
				yes := rf.peers[server].Call("Raft.ReceiveMessage", args, reply)
				if yes == true && reply.Success == true {
					rf.mu.Lock()
					rf.nextIndex[server] = len(rf.log)+1
					rf.matchIndex[server] = len(rf.log)
					rf.successNum++
					rf.mu.Unlock()
				}
			}
		}
	}

	if rf.successNum > len(rf.peers)/2 {
		rf.successNum = 0
		for i := rf.commitIndex; i < len(rf.log); i++ {
			rf.mu.Lock()
			rf.commitIndex++
			rf.mu.Unlock()
			var applied ApplyMsg
			applied.CommandValid = true
			rf.mu.RLock()
			applied.Command = rf.log[i].Command
			rf.mu.RUnlock()
			applied.CommandIndex = i+1
			rf.applyCh <- applied
			rf.lastApplied++
		}

		args.LeaderCommit = rf.commitIndex
		args.Entries = []LogEntries{}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me{
				continue
			}
			go rf.sendCommitRPC(i, args, reply)
		}
		
	}
}

func (rf *Raft) sendCommitRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.peers[server].Call("Raft.ReceiveMessage", args, reply)
	if reply.Success == false {
	}
}

func (rf *Raft) StartAgreement(command interface{}) {
	if rf.killed() {
		return
	}

	var log_entry LogEntries
	log_entry.Command = command

	rf.mu.Lock()
	log_entry.Term = rf.currentTerm
	rf.log = append(rf.log, log_entry)
	rf.successNum = 1
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
	rf.nextIndex[rf.me]++
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}

		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		if len(rf.log) == 1 {
			args.PrevLogIndex = len(rf.log) - 1		// 1-indexed
			args.PrevLogTerm = rf.log[len(rf.log)-1].Term
		} else {
			args.PrevLogIndex = len(rf.log) - 1
			args.PrevLogTerm = rf.log[len(rf.log)-2].Term
		}
		args.Entries = []LogEntries{log_entry}
		args.LeaderCommit = rf.commitIndex

		go rf.sendAppendEntries(i, &args, &reply)

	}
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
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader == false {
		return index, term, false // returns false
	}
	rf.mu.Lock()
	rf.successNum = 0
	rf.mu.Unlock()
	
	go rf.StartAgreement(command)

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

func (rf *Raft) heartTimeOut(timeoutVal int) {
	// need to calculate some timeout value -> greater than 150-300ms
	random := rand.Intn(401) + 1000
	timeout := time.Duration(random) * time.Millisecond
	time.Sleep(timeout)

	if rf.killed() {
		return
	}

	// need to see if heartbeat timed out
	// if it did, then call an election
	if time.Since(rf.lastHeartBeat) >= timeout && time.Since(rf.voteTimeOut) >= timeout {
		rf.mu.Lock()
		rf.serverState = "Candidate"
		rf.mu.Unlock()
		go rf.startElection()
	}
}

func (rf *Raft) sendHeartBeats(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.peers[server].Call("Raft.ReceiveMessage", args, reply)
}

func (rf *Raft) checkLeader() {
	// need to see if leader or not
	timeout := 250 * time.Millisecond
	time.Sleep(timeout)

	if rf.numVotes > len(rf.peers)/2 {
		rf.mu.Lock()
		rf.serverState = "Leader"
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log) + 1
		}
		rf.mu.Unlock()

	nestedFor:
		for {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				} else {
					if rf.killed() {
						return
					} else {

						// send rpc
						args := AppendEntriesArgs{}
						reply := AppendEntriesReply{}

						args.Term = rf.currentTerm
						args.LeaderId = rf.me

						if len(rf.log) > 0 {
							args.PrevLogIndex = len(rf.log)
							args.PrevLogTerm = rf.log[len(rf.log)-1].Term
						} else {
							args.PrevLogIndex = 0
							args.PrevLogTerm = 0
						}

						args.Entries = []LogEntries{}
						args.LeaderCommit = rf.commitIndex

						_, isleader := rf.GetState()

						// if the server is no longer the leader, stop sending rpc
						if isleader == false {
							break nestedFor
						}

						go rf.sendHeartBeats(i, &args, &reply)
					}
				}

				_, isleader := rf.GetState()

				// if the server is no longer the leader, stop sending rpc
				if isleader == false {
					break nestedFor
				}
			}
			time.Sleep(150 * time.Millisecond)
		}
	} else if rf.serverState == "Candidate" {
		rf.startElection()
	} else {
		random := rand.Intn(301)
		go rf.heartTimeOut(random)
	}
}

func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}

	go rf.checkLeader()

	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.numVotes = 1
	rf.voteTimeOut = time.Now()
	rf.mu.Unlock()

	// need to send requestvote rpc's
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue // skip sending rpc to same server
		}

		args := RequestVoteArgs{}
		reply := RequestVoteReply{}

		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		if len(rf.log) > 0 {
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
			args.LastLogIndex = len(rf.log) // 1-indexed
		} else {
			args.LastLogTerm = 0
			args.LastLogIndex = 0
		}

		go rf.sendRequestVote(i, &args, &reply)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (2A, 2B).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.numVotes = 0
	rf.serverState = "Follower"
	rf.lastHeartBeat = time.Time{}
	rf.voteTimeOut = time.Time{}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}

	rf.applyCh = applyCh
	random := rand.Intn(301)
	go rf.heartTimeOut(random)

	return rf
}