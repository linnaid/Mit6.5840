package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type LogEntry struct {
	Term int
	Command interface{}
}

type State int 
const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	currentTerm int // 当前任期号
	votedFor int // 当前任期内候选者id
	state State // 当前身份
	electionTimer *time.Timer // 控制选举超时
	heartbeatInterval time.Duration // 定期发送心跳的间隔

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
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
	// Your code here (3C).
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
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int // leader任期号
	CandidateId int // 候选者id
	// 下面是日志
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int 
	VoteGranted bool // 是否投票给当前任期者
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// fmt.Printf("已开始投票...")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// fmt.Println("term1:", reply.Term)
	// fmt.Println("state:", rf.state)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = -1
		rf.resetElectionTimer()
		// fmt.Println("term2:", reply.Term)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
		rf.resetElectionTimer()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.resetElectionTimer()
		reply.Success = true
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int 
	// 下面是日志	
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry  // 需要追加的日志条目，为空则是心跳
	LeaderCommit int    // Leader的已提交索引
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft)sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	// Your code here (3B).


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

// 辅助函数
func (rf *Raft)startElection() {
	// fmt.Printf("已开始选举...")
	rf.mu.Lock()
	// fmt.Println("state0:",rf.state)
	rf.state = Candidate
	// fmt.Println("state:",rf.state)
	rf.currentTerm++
	// fmt.Println("term:",rf.currentTerm)
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.mu.Unlock()
	// 补充字段
	args := &RequestVoteArgs{
		Term : term,
		CandidateId: rf.me,
		LastLogIndex: 0,
		LastLogTerm: 0,
	}
	var votes int32 = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				// 处理
				// fmt.Printf("rpc调用成功...")
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Candidate || rf.currentTerm != term {
					return 
				}
				if reply.Term > term {
					rf.resetElectionTimer()
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					return
				}
				
				if reply.VoteGranted && reply.Term == term {
					newnode := atomic.AddInt32(&votes, 1)
					// fmt.Println("votes:",newnode)
					if int(newnode) > len(rf.peers)/2{
						if reply.Term == term && rf.state == Candidate {
							rf.state = Leader
							// go rf.sendHeartbeats()	//////////////////////////////
						}
					}
				}
			} else {
				return
			}			
		}(i)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed(){

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		// is_leader := false
		state := rf.state
		rf.mu.Unlock()
		
		switch state {
		case Follower, Candidate:
			// 检测选举是否超时
			select {
			case <-rf.electionTimer.C:
				rf.startElection()
			default:
				time.Sleep(10 * time.Millisecond)
			}
		case Leader:
			rf.mu.Lock()
			if rf.electionTimer != nil {
				if !rf.electionTimer.Stop() {
					select {
					case <-rf.electionTimer.C:
					default:
					}
				}
			}
			term := rf.currentTerm
			rf.mu.Unlock()
			// 定期发送心跳
			for i := range rf.peers {
				if i == rf.me {
					continue
				} else {
					args := &AppendEntriesArgs{
						Term: term,
						LeaderId: rf.me,
						Entries: nil,
					}
					go func(server int, args *AppendEntriesArgs) {
						reply := &AppendEntriesReply{}
						ok := rf.sendAppendEntries(server, args, reply)
						if ok {
							if reply.Term > term {
								rf.mu.Lock()
								if reply.Term > rf.currentTerm {
									rf.state = Follower
									rf.currentTerm = reply.Term
									rf.votedFor = -1
									rf.resetElectionTimer()
								}
								rf.mu.Unlock()
							}
						}
					}(i, args)
				}
			}
			time.Sleep(rf.heartbeatInterval)
		}

		// if is_leader {
		// 	go rf.sendHeartbeats()
		// }
	}
	// pause for a random amount of time between 50 and 350
	// milliseconds.
	// ms := 50 + (rand.Int63() % 300)
	// time.Sleep(time.Duration(ms) * time.Millisecond)
}

// 辅助函数
func (rf *Raft) resetElectionTimer() {
	if rf.electionTimer != nil {
		if !rf.electionTimer.Stop() {
			select {
			case <-rf.electionTimer.C:
			default:
			}
		}
	}
	t_out := time.Duration(300 + rand.Intn(200)) * time.Millisecond
	rf.electionTimer = time.NewTimer(t_out)
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
// 初始化
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.resetElectionTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
