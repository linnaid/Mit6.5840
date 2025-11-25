package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	// "go.etcd.io/etcd/client/v3/snapshot"
)

type LogEntry struct {
	Term    int
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
	currentTerm       int           // 当前任期号
	votedFor          int           // 当前任期内候选者id
	state             State         // 当前身份
	electionTimer     *time.Timer   // 控制选举超时
	heartbeatInterval time.Duration // 定期发送心跳的间隔

	// 日志
	log         []LogEntry
	commitIndex int                   // 已知被提交的最大日志记录索引值
	lastApplied int                   // 被执行的最大日志索引号
	nextIndex   []int                 // 每一个服务器下一个日志索引号(初使化为领导者的最后一条日志索引号+1)
	matchIndex  []int                 // 每一个服务器已经复制到该服务器的最大索引号(初始化为0，单调递增)
	applyCh     chan raftapi.ApplyMsg // 管道，发送可执行日志消息

	// 快照
	lastIncludeIndex int    // 快照中最后一条日志的索引
	lastIncludeTerm  int    // 快照中最后一条日志的任期
	snapshot         []byte // 储存来自上层 kvserver 的快照
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)

	data := w.Bytes()
	rf.persister.Save(data, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		// fmt.Printf("server %v readPersist failed\n",rf.me)
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludeIndex = lastIncludeIndex
	rf.lastIncludeTerm = lastIncludeTerm
	///////////////////////////////
	
	if len(log) > 0 {
		rf.log = log
	} else {
		rf.log = []LogEntry{{
			Term: rf.lastIncludeTerm,
		}}
	}

	rf.commitIndex = rf.lastIncludeIndex
	rf.lastApplied = rf.lastIncludeIndex

	rf.snapshot = rf.persister.ReadSnapshot()

}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 这里开始是第一处添加的代码---------------------------------------------
type InstallSnapshotArgs struct {
	Term             int
	LeaderID         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte // 快照内容
}

type InstallSnapshotReply struct {
	Term int
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	// fmt.Printf("我要调用这个函数了\n")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludeIndex {
		return
	}
	lastIndex := rf.lastIncludeIndex + len(rf.log) - 1
	if lastIndex < index {
		index = lastIndex
	}

	new_term := rf.log[index-rf.lastIncludeIndex].Term
	new_log := make([]LogEntry, 0)
	new_log = append(new_log, LogEntry{Term: new_term})
	if index-rf.lastIncludeIndex+1 < len(rf.log) {
		new_log = append(new_log, rf.log[index+1-rf.lastIncludeIndex:]...)
	}

	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = new_term
	rf.log = new_log
	rf.snapshot = snapshot

	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	// fmt.Println(rf.lastIncludeIndex, index)
	rf.persist()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	// fmt.Printf("sendAppendEntries函数进入...\n")
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("已进入函数InstallSnapshot...")

	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	} else if args.Term < rf.currentTerm {
		// fmt.Printf("被返回...")
		return
	}
	rf.resetElectionTimer()

	if args.LastIncludeIndex <= rf.lastIncludeIndex {
		// fmt.Printf("被返回...")
		// fmt.Println(args.LastIncludeIndex, rf.lastIncludeIndex)
		return
	}

	//////////////////////////////////////////////////////////////////
	if args.LastIncludeIndex < len(rf.log)+rf.lastIncludeIndex-1 {
		startId := args.LastIncludeIndex + 1 - rf.lastIncludeIndex
		if startId > 0 && startId <= len(rf.log) {
			// 检查保留的日志是否与快照一致
			if startId-1 >= 0 && startId-1 < len(rf.log) && rf.log[startId-1].Term == args.LastIncludeTerm {
				new_log := make([]LogEntry, 0)
				new_log = append(new_log, LogEntry{Term: args.LastIncludeTerm})
				new_log = append(new_log, rf.log[startId:]...)
				rf.log = new_log
			} else {
				// 如果不一致，丢弃所有日志
				rf.log = []LogEntry{{
					Term: args.LastIncludeTerm,
				}}
			}
		} else {
			rf.log = []LogEntry{{
				Term: args.LastIncludeTerm,
			}}
		}
	} else {
		rf.log = []LogEntry{{
			Term: args.LastIncludeTerm,
		}}
	}

	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.snapshot = args.Data

	// Update commitIndex
	if rf.commitIndex < rf.lastIncludeIndex {
		rf.commitIndex = rf.lastIncludeIndex
	}
	if rf.lastApplied < rf.lastIncludeIndex {
	}

	rf.persist()
}
// 到这里结束--------------------------------

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int // leader任期号
	CandidateId int // 候选者id
	// 下面是日志
	LastLogIndex int // 最新一条日志索引
	LastLogTerm  int // 最新日志任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
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
		rf.persist()
		rf.resetElectionTimer()
		// fmt.Println("term2:", reply.Term)
	}

	upToDate := false
	// fmt.Println(rf.lastIncludeIndex)
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
		(args.LastLogIndex >= rf.lastIncludeIndex+len(rf.log)-1 && args.LastLogTerm == rf.log[len(rf.log)-1].Term) {
		upToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
		rf.resetElectionTimer()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 3A
	// fmt.Printf("已开始AppendEntries函数...\n")
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		// rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		rf.resetElectionTimer()
	} else {
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		rf.resetElectionTimer()
	}

	// 3B
	if args.PrevLogIndex < rf.lastIncludeIndex {
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludeIndex + 1
		reply.ConflictTerm = -1
		// rf.mu.Unlock()
		return
	}

	lastIndex := len(rf.log) + rf.lastIncludeIndex - 1
	if args.PrevLogIndex > rf.lastIncludeIndex {
		sliceIdx := args.PrevLogIndex - rf.lastIncludeIndex
		if sliceIdx >= len(rf.log) {
			reply.Success = false
			reply.ConflictTerm = -1
			reply.ConflictIndex = lastIndex + 1
			// rf.mu.Unlock()
			return
		}
		if rf.log[sliceIdx].Term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictTerm = rf.log[sliceIdx].Term
			conflictIdx := sliceIdx
			for conflictIdx > 0 && rf.log[conflictIdx-1].Term == reply.ConflictTerm {
				conflictIdx--
			}
			reply.ConflictIndex = rf.lastIncludeIndex + conflictIdx
			// rf.mu.Unlock()
			return
		}
	} else if args.PrevLogIndex == rf.lastIncludeIndex {
		if args.PrevLogTerm != rf.lastIncludeTerm {
			reply.Success = false
			reply.ConflictIndex = rf.lastIncludeIndex
			reply.ConflictTerm = -1
			// rf.mu.Unlock()
			return
		}
	}

	reply.Success = true

	// Append new entries: delete any conflicting entries and append new ones
	// According to Raft paper: if PrevLogIndex and PrevLogTerm match,
	// delete all entries from PrevLogIndex+1 onwards and append new entries
	if len(args.Entries) > 0 {
		idx := args.PrevLogIndex + 1 - rf.lastIncludeIndex
		if idx < 0 {
			idx = 0
		} 
		// else if idx > len(rf.log) {
		// 	idx = len(rf.log)
		// }
		// Delete all entries from PrevLogIndex+1 onwards and append new entries
		// if idx < len(rf.log) {
		// 	rf.log = rf.log[:idx]
		// }
		// 检查是否有冲突
		conflictFound := false
		for i, entry := range args.Entries {
			logIndex := idx + i
			if logIndex < len(rf.log) {
				if rf.log[logIndex].Term != entry.Term {
					// 发现冲突，截断并追加
					rf.log = rf.log[:logIndex]
					rf.log = append(rf.log, args.Entries[i:]...)
					rf.persist()
					conflictFound = true
					break
				}
			} else {
				// 追加新条目
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				conflictFound = true
				break
			}
		}
		
		// 如果没有冲突且所有条目都已存在，不需要持久化
		if !conflictFound {
			// 所有条目都已存在且匹配，不需要修改
			rf.persist()
		}
		// rf.log = append(rf.log, args.Entries...)
		// rf.persist()
		// for i := 0; i < len(args.Entries); i++ {
		// 	if i + idx >= len(rf.log) {
		// 		rf.log = append(rf.log, args.Entries[i:]...)
		// 		break;
		// 	} else {
		// 		if rf.log[i + idx].Term != args.Entries[i].Term {
		// 			// fmt.Printf("任期不同...\n")
		// 			// fmt.Println(len(rf.log), rf.me, rf.log)
		// 			rf.log = rf.log[:i+idx]
		// 			// fmt.Println(len(rf.log), rf.me, args.Entries[i])
		// 			rf.log = append(rf.log, args.Entries[i:]...)
		// 			// fmt.Println(len(rf.log), rf.me, rf.log)
		// 			break;
		// 		}
		// 	}
		// }
	}

	// if rf.lastApplied >= len(rf.log) {
	// 	rf.lastApplied = len(rf.log) - 1
	// 	// rf.lastApplied = rf.commitIndex - 1
	// }

	// fmt.Println(args.LeaderCommit, rf.commitIndex)
	// lastIndexAfter := rf.lastIncludeIndex + len(rf.log) - 1
	// // if args.LeaderCommit > rf.commitIndex {
	// 	Commit := rf.commitIndex
	// 	for i := rf.commitIndex+1; i <= min(args.LeaderCommit, lastIndexAfter); i++ {
	// 		real_i := i - rf.lastIncludeIndex
	// 		if real_i > 0 && real_i < len(rf.log) && rf.log[real_i].Term == rf.currentTerm {
	// 			Commit = i
	// 		}
	// 	}
	// 	if Commit > rf.commitIndex {
	// 		rf.commitIndex = Commit
	// 	}
	// fmt.Printf("变量:\n")
	// fmt.Println(args.LeaderCommit, len(rf.log))
	// }

	// Update commitIndex: follower can safely update to min(LeaderCommit, lastNewIndex)
	// because leader only commits entries from its current term
	if args.LeaderCommit > rf.commitIndex {
		lastNewIdx := rf.lastIncludeIndex + len(rf.log) - 1
		if args.LeaderCommit < lastNewIdx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIdx
		}
		// Ensure commitIndex doesn't go below lastIncludeIndex
		if rf.commitIndex < rf.lastIncludeIndex {
			rf.commitIndex = rf.lastIncludeIndex
		}
	}

	// if args.LeaderCommit < lastNewIdx {
	// 	rf.commitIndex = args.LeaderCommit
	// } else {
	// 	rf.commitIndex = lastIndex

	// }

	// rf.mu.Unlock()

	// if rf.lastApplied > rf.commitIndex {
	// 	rf.lastApplied = rf.commitIndex
	// }
	if rf.commitIndex < rf.lastIncludeIndex {
		rf.commitIndex = rf.lastIncludeIndex
	}
	if rf.lastApplied < rf.lastIncludeIndex {
		rf.lastApplied = rf.lastIncludeIndex
	}

	// fmt.Printf("lastAppend和commitIndex分别是:")
	// fmt.Println(rf.lastApplied, rf.commitIndex)
	// rf.mu.Lock()
	// msgs := []raftapi.ApplyMsg{}
	// for rf.lastApplied < rf.commitIndex {
	// 	// fmt.Printf("进入循环\n")
	// 	rf.lastApplied++
	// 	if rf.lastApplied <= rf.lastIncludeIndex {
	// 		rf.lastApplied = rf.lastIncludeIndex
	// 		continue
	// 	}
	// 	sliceId := rf.lastApplied - rf.lastIncludeIndex
	// 	if sliceId <= 0 || sliceId >= len(rf.log) {
	// 		last := rf.lastIncludeIndex + len(rf.log) - 1
	// 		if last < rf.commitIndex {
	// 			rf.commitIndex = last
	// 		}
	// 		break
	// 	}
	// 	msgs = append(msgs, raftapi.ApplyMsg{
	// 		CommandValid: true,
	// 		Command: rf.log[sliceId].Command,
	// 		CommandIndex: rf.lastApplied,
	// 	})
	// 	// fmt.Println(rf.log[rf.lastApplied].Command)
	// 	// fmt.Printf("1")
	// }

	// // rf.mu.Unlock()
	// for _, msg := range msgs {
	// 	// fmt.Printf("要开始应用到上层了~\n")
	// 	// fmt.Println(msg.CommandIndex)
	// 	// fmt.Println("lastApplied: ",rf.lastApplied)
	// 	// fmt.Println("lastIncludeIndex: ",rf.lastIncludeIndex)
	// 	// fmt.Printf("AppendEntries发送msg\n")
	// 	// fmt.Println(rf.lastApplied, rf.lastIncludeIndex)
	// 	rf.applyCh<-msg
	// }
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// 下面是日志
	PrevLogIndex int        // 之前处理的日志索引
	PrevLogTerm  int        // 日志任期号
	Entries      []LogEntry // 需要追加的日志条目，为空则是心跳
	LeaderCommit int        // Leader的已提交索引
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// fmt.Printf("sendAppendEntries函数进入...\n")
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
	// index := -1
	// new_index := len(rf.log) + rf.lastIncludeIndex - 1
	term := -1
	isLeader := true
	// fmt.Printf("已开始Start函数...\n")
	// Your code here (3B).、
	rf.mu.Lock()
	// fmt.Printf("第一个锁成功进入...\n")
	if rf.state != Leader {
		isLeader = false
		rf.mu.Unlock()
		return -1, term, isLeader
	} /*else {*/
	term = rf.currentTerm
	// index = len(rf.log)
	entry := LogEntry{
		Command: command,
		Term:    term,
	}

	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, entry)
	new_index := len(rf.log) + rf.lastIncludeIndex - 1
	// fmt.Printf("Leader状态：")
	// fmt.Println(len(rf.log), rf.me, rf.log)
	// rf.commitIndex++
	rf.persist()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			rf.mu.Lock()
			rf.nextIndex[i] = new_index + 1
			rf.matchIndex[i] = new_index
			rf.mu.Unlock()
			continue
		}

		go func(server int) {
			for !rf.killed() {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				Index := rf.nextIndex[server] - 1
				// fmt.Println(Index, index,  rf.lastIncludeIndex)
				// return

				if Index+1 <= rf.lastIncludeIndex {
					// fmt.Printf("已进入分支...")
					args := InstallSnapshotArgs{
						Term:             rf.currentTerm,
						LeaderID:         rf.me,
						LastIncludeIndex: rf.lastIncludeIndex,
						LastIncludeTerm:  rf.lastIncludeTerm,
						Data:             rf.snapshot,
					}
					reply := InstallSnapshotReply{}

					rf.mu.Unlock()
					ok := rf.sendInstallSnapshot(server, &args, &reply)
					if ok {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
							rf.persist()
							rf.mu.Unlock()
							return
						}
						Index = rf.lastIncludeIndex
						rf.nextIndex[server] = Index + 1
						rf.matchIndex[server] = Index
						rf.mu.Unlock()
					}
					// rf.mu.Unlock()
					time.Sleep(10 * time.Millisecond)
					continue
				} else {
					rf.mu.Unlock()
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
				}

				if Index == rf.lastIncludeIndex {
					args.PrevLogIndex = rf.lastIncludeIndex
					args.PrevLogTerm = rf.lastIncludeTerm
				} else /*if Index > rf.lastIncludeIndex*/ {
					args.PrevLogIndex = Index
					sliceIdx := Index - rf.lastIncludeIndex
					if sliceIdx <= 0 || sliceIdx >= len(rf.log) {
						args.PrevLogTerm = rf.lastIncludeTerm
					} else {
						args.PrevLogTerm = rf.log[sliceIdx].Term
					}
				}

				real_Index := rf.nextIndex[server] - rf.lastIncludeIndex
				if real_Index < 1 {
					real_Index = 1
					// args.PrevLogIndex = -1
					// args.PrevLogTerm = 0
				} else if real_Index > len(rf.log) {
					real_Index = len(rf.log)
					// Index = len(rf.log) - 1
					// args.PrevLogTerm = rf.log[Index].Term
					// args.PrevLogIndex = Index
				} //else {
				// 	args.PrevLogIndex = Index
				// 	args.PrevLogTerm = rf.log[Index].Term
				// }
				entries := make([]LogEntry, len(rf.log[real_Index:]))
				copy(entries, rf.log[real_Index:])

				// else {
				// 	args.PrevLogIndex = rf.nextIndex[server] - 1
				// 	if args.PrevLogIndex == -1 {
				// 		args.PrevLogTerm = 0
				// 	} else {
				// 		args.PrevLogTerm = rf.log[rf.nextIndex[server]-1].Term
				// 	}
				// 	////////////////////////////////////////////////////////////
				// 	// fmt.Println(rf.me, len(rf.log))
				// }
				// if rf.nextIndex[server] < len(rf.log) {
				// 	args.Entries = rf.log[rf.nextIndex[server]:]
				// } else {
				// 	args.Entries = []LogEntry{}
				// }
				// if rf.nextIndex[server] < 0 {
				// 	rf.nextIndex[server] = 0
				// }
				// if rf.nextIndex[server] > len(rf.log) {
				// 	rf.nextIndex[server] = len(rf.log)
				// }
				// args.Entries = rf.log[rf.nextIndex[server]:]
				args.Entries = entries

				rf.mu.Lock()
				reply := AppendEntriesReply{}
				if rf.state != Leader || args.Term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(server, &args, &reply)
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.persist()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					if reply.Success {
						// fmt.Printf("reply.Success返回正确\n")
						// fmt.Println(index, rf.matchIndex[server], server, rf.me)
						rf.mu.Lock()
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						rf.mu.Unlock()
						rf.updateCommiIndex()
						// fmt.Println(rf.matchIndex[server], rf.nextIndex[server], server)
						return
					} else {
						rf.mu.Lock()
						if reply.ConflictTerm == -1 {
							rf.nextIndex[server] = reply.ConflictIndex
							// return
						} else {
							lastIndexOfTerm := -1
							for i := len(rf.log) - 1; i >= 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = i + rf.lastIncludeIndex
									break
								}
							}
							if lastIndexOfTerm >= 0 {
								rf.nextIndex[server] = lastIndexOfTerm + 1
							} else {
								rf.nextIndex[server] = reply.ConflictIndex
							}
						}
						rf.mu.Unlock()
						time.Sleep(10 * time.Millisecond)
						continue
					}
				} else {
					time.Sleep(time.Microsecond * 10)
					continue
				}
			}
		}(i)
	}

	// time.Sleep(2*time.Millisecond)

	// rf.mu.Lock()
	// fmt.Println(rf.lastApplied, rf.commitIndex)
	// msgs := []raftapi.ApplyMsg{}
	// for rf.lastApplied < rf.commitIndex {
	// 	// fmt.Printf("发送msg\n")
	// 	rf.lastApplied++
	// 	if rf.lastApplied < rf.lastIncludeIndex {
	// 		rf.lastApplied = rf.lastIncludeIndex
	// 		continue
	// 	}
	// 	sliceIdx := rf.lastApplied - rf.lastIncludeIndex
	// 	if sliceIdx <= 0 || sliceIdx > len(rf.log) {
	// 		rf.lastApplied--
	// 		continue
	// 	}
	// 	msgs = append(msgs, raftapi.ApplyMsg{
	// 		CommandValid: true,
	// 		Command: rf.log[sliceIdx].Command,
	// 		CommandIndex: rf.lastApplied,
	// 	})
	// }

	// rf.mu.Unlock()
	// for _, msg := range msgs {
	// 	fmt.Printf("Start发送msg\n")
	// 	fmt.Println("lastApplied: ",rf.lastApplied)
	// 	fmt.Println("lastIncludeIndex: ",rf.lastIncludeIndex)
	// 	rf.applyCh<-msg
	// }
	// }

	return new_index, term, isLeader
}
// func (rf *Raft) Start(command interface{}) (int, int, bool) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	if rf.state != Leader {
// 		return -1, rf.currentTerm, false
// 	}
// 	index := rf.lastIncludeIndex + len(rf.log) - 1
// 	rf.log = append(rf.log, LogEntry{
// 		Command: command,
// 		Term:    rf.currentTerm,
// 	})
// 	rf.persist()
// 	return index, rf.currentTerm, true
// }




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
func (rf *Raft) startElection() {
	// fmt.Printf("已开始选举...\n")
	rf.mu.Lock()
	// fmt.Println("state0:",rf.state)
	rf.state = Candidate
	// fmt.Println("state:",rf.state)
	rf.currentTerm++
	// fmt.Println("term:",rf.currentTerm)
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
	lastLogIndex := rf.lastIncludeIndex + len(rf.log) - 1
	lastLogTerm := rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()
	// 补充字段
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
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

				if rf.state != Candidate || rf.currentTerm != args.Term {
					return
				}
				if reply.Term > term {
					rf.resetElectionTimer()
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				if reply.VoteGranted && reply.Term == term {
					newnode := atomic.AddInt32(&votes, 1)
					// fmt.Println("votes:",newnode)
					if int(newnode) > len(rf.peers)/2 {
						if reply.Term == term && rf.state == Candidate {
							rf.state = Leader
							// go rf.sendHeartbeats()	//////////////////////////////
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))

							for j := range rf.peers {
								rf.nextIndex[j] = lastLogIndex + 1
								rf.matchIndex[j] = rf.lastIncludeIndex
							}
						}
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) ticker() {
	// fmt.Printf("已开始ticker函数...\n")
	for !rf.killed() {
		// fmt.Printf("已开始循环...\n")
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
					rf.mu.Lock()
					prevLogIndex := rf.nextIndex[i] - 1
					var prevLogTerm int
					if prevLogIndex < rf.lastIncludeIndex {
						prevLogIndex = rf.lastIncludeIndex
						prevLogTerm = rf.lastIncludeTerm
					} else if prevLogIndex == rf.lastIncludeIndex {
						prevLogTerm = rf.lastIncludeTerm
					} else {
						sliceIdx := prevLogIndex - rf.lastIncludeIndex
						if sliceIdx > 0 && sliceIdx < len(rf.log) {
							prevLogTerm = rf.log[sliceIdx].Term
						} else {
							prevLogIndex = rf.lastIncludeIndex
							prevLogTerm = rf.lastIncludeTerm
						}
					}
					args := &AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
					}
					rf.mu.Unlock()
					///////////////////////////////////////////////////////////////////
					// if args.PrevLogIndex < 0 {
					// 	args.PrevLogTerm = 0
					// } else if args.PrevLogIndex < len(rf.log) {
					// 	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					// } else {
					// 	args.PrevLogIndex = -1
					// 	args.PrevLogTerm = 0
					// }
					go func(server int, args *AppendEntriesArgs) {
						// fmt.Printf("发送心跳\n")
						// fmt.Println(rf.commitIndex)
						reply := &AppendEntriesReply{}
						ok := rf.sendAppendEntries(server, args, reply)
						if ok {

							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								
								if reply.Term > rf.currentTerm {
									rf.state = Follower
									rf.currentTerm = reply.Term
									rf.votedFor = -1
									rf.persist()
									rf.resetElectionTimer()
								}
							}
							rf.mu.Unlock()
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
	// // pause for a random amount of time between 50 and 350
	// // milliseconds.
	// ms := 50 + (rand.Int63() % 300)
	// time.Sleep(time.Duration(ms) * time.Millisecond)
}

// 辅助函数
func (rf *Raft) updateCommiIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastIndex := rf.lastIncludeIndex + len(rf.log) - 1
	for N := lastIndex; N > rf.commitIndex; N-- {
		if N <= rf.lastIncludeIndex {
			break
		}
		count := 1
		for i := range rf.peers {
			// fmt.Println(rf.matchIndex[i])
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		// fmt.Printf("count是：%d\n",count)
		// fmt.Println(len(rf.peers)/2)

		if count > len(rf.peers)/2 {
			sliceIdx := N - rf.lastIncludeIndex
			if sliceIdx > 0 && sliceIdx < len(rf.log) && rf.log[sliceIdx].Term == rf.currentTerm {
				rf.commitIndex = N
				break
			}
			// fmt.Println(rf.commitIndex)
			// fmt.Printf("修改commitIndex\n")
			// fmt.Println(rf.commitIndex)
		}
	}
}

func (rf *Raft) resetElectionTimer() {
	if rf.electionTimer != nil {
		if !rf.electionTimer.Stop() {
			select {
			case <-rf.electionTimer.C:
			default:
			}
		}
	}
	t_out := time.Duration(300+rand.Intn(200)) * time.Millisecond
	rf.electionTimer = time.NewTimer(t_out)
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// 快照日志：如果 lastApplied < lastIncludeIndex，需要发送快照
		if rf.lastApplied < rf.lastIncludeIndex {
			snapshotIndex := rf.lastIncludeIndex
			snapshotTerm := rf.lastIncludeTerm
			snapshotData := make([]byte, len(rf.snapshot))
			copy(snapshotData, rf.snapshot)

			// 更新 lastApplied 为快照索引，这样测试框架的 ingestSnap 会正确更新
			rf.lastApplied = snapshotIndex
			rf.mu.Unlock()
			rf.applyCh <- raftapi.ApplyMsg{
				SnapshotValid: true,
				SnapshotIndex: snapshotIndex,
				SnapshotTerm:  snapshotTerm,
				Snapshot:      snapshotData,
			}
			continue
		}

		// 普通日志：只应用 commitIndex 之后且不在快照中的日志
		msgs := make([]raftapi.ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex {
			nextIdx := rf.lastApplied + 1
			// 如果 nextIdx 在快照范围内，说明快照还没有被发送
			// 这种情况不应该发生，因为快照应该在前面被发送
			// 但为了安全，我们跳过这些条目
			if nextIdx <= rf.lastIncludeIndex {
				rf.lastApplied = rf.lastIncludeIndex
				continue
			}
			sliceId := nextIdx - rf.lastIncludeIndex
			if sliceId <= 0 || sliceId >= len(rf.log) {
				// No more entries to apply
				break
			}
			rf.lastApplied = nextIdx
			entry := rf.log[sliceId]
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
		// 防止空转
		time.Sleep(10 * time.Millisecond)
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
// 初始化
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 1)
	rf.log[0].Command = nil
	rf.log[0].Term = 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.heartbeatInterval = 100 * time.Millisecond
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.applier()

	rf.resetElectionTimer()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
