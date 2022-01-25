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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

	// leader election.
	// persistent state on all servers.
	currentTerm int        // 当前任期
	votedFor    int        // 投票的候选人id，-1表示未投票
	log         []LogEntry // 初始索引为1，通过填充一条空entry实现
	// volatile state on all servers.
	commitIndex int // 该peer已知的最后commit的log entry的索引
	lastApplied int // 应用到该peer状态机的log entry的最新索引
	// volatile state on leaders.
	// 每次选举后重新初始化
	nextIndex  []int // 每个follower的应该接收的下一条log entry的索引，初始化为leader last log index + 1
	matchIndex []int // 每个follower已知的已复制的log entry的最新索引，齿梳化为0

	// election timeout相关
	lastFlushTime   int64
	electionTimeOut int64
	// 当前状态，0follower，1candidate，2leader
	state int
	// apply通道
	applyCh chan ApplyMsg
}

// log entry struct.
type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == 2)
	rf.mu.Unlock()
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
	Term         int // candidate's term
	CandidateId  int
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 设置回复的默认值
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm < args.Term {
		// 如果对方的term更优，将peer的term更新到对方的term，同时转换状态为follower，重置其投票状态为-1
		rf.currentTerm = args.Term
		rf.setFollower()
	}

	if rf.currentTerm > args.Term {
		// peer的term更优，投反对票，提前退出
		reply.VoteGranted = false
		return
	}

	if (rf.votedFor == -1) || (rf.votedFor == args.CandidateId) {
		// peer尚未投票或投票对象为对方
		// candidate或leader状态，peer的投票状态应该都为自己，不会进入该判断
		if (args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
			((args.LastLogTerm == rf.log[len(rf.log)-1].Term) && (args.LastLogIndex >= len(rf.log)-1)) {
			// 对方的log任期更新，或对方的log任期相同且log index大于等于peer
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.flushElectionTimer()
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) setFollower() {
	rf.state = 0
	rf.votedFor = -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 设置回复的默认值
	reply.Term = rf.currentTerm
	reply.Success = false

	if rf.currentTerm <= args.Term {
		// 如果对方term未过期，刷新选举timer
		rf.flushElectionTimer()

		if rf.currentTerm < args.Term {
			// 如果对方的term更优，将peer的term更新到对方的term，同时转换状态为follower
			rf.currentTerm = args.Term
			rf.setFollower()
		}
	}

	// 下面都是append entries的判断
	if args.Term < rf.currentTerm {
		// leader的任期过期，拒绝
		// fmt.Println("过期，拒绝")
		reply.Success = false
		return
	}

	if len(rf.log)-1 < args.PrevLogIndex {
		// PrevLogIndex超过peer的log索引上限，拒绝
		// fmt.Printf("PrevLogIndex超过peer的log索引上限，拒绝. pidx: %d, loglastidx: %d\n", args.PrevLogIndex, len(rf.log)-1)
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 出现冲突，删除peer冲突后的所有log，拒绝
		// fmt.Println("出现冲突，拒绝")
		reply.Success = false
		return
	}

	// 如果以上检查都通过，进入追加entries阶段
	reply.Success = true
	// 在peer的log中写入参数中的entries
	// 这里需要满足幂等性，已经存在的entry将跳过
	// 冲突的entry，将删除peer的对应entry及其随后entries
	// 空entries，比如心跳请求，将跳过
	log_idx := -1
	for idx := range args.Entries {
		log_idx = args.PrevLogIndex + idx + 1
		if log_idx >= len(rf.log) {
			// 该idx位置开始的entries为新增entries，追加到peer的log后
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}

		if args.Entries[idx].Term != rf.log[log_idx].Term {
			// 冲突发生了
			// fmt.Println("擦除冲突数据")
			rf.log = rf.log[:log_idx]
			// 再将后续entries追加
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}

	// 判断peer的commit是否需要更新
	if args.LeaderCommit < len(rf.log) && args.LeaderCommit > rf.commitIndex {
		// 更新peer的commitIndex
		if args.LeaderCommit < args.PrevLogIndex+len(args.Entries) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}
	}
	// fmt.Printf("%d from %d: my log length %d, received entries length %d; my commit %d, leader commit %d; my term %d, leader term %d, pidx %d, pt %d\n",
	// 	rf.me, args.LeaderId, len(rf.log), len(args.Entries), rf.commitIndex, args.LeaderCommit, rf.currentTerm, args.Term, args.PrevLogIndex, args.PrevLogTerm)
}

func prependEntry(x []LogEntry, y LogEntry) []LogEntry {
	x = append(x, LogEntry{})
	// 将x数组shift后移一位，0位置不变
	copy(x[1:], x)
	x[0] = y
	return x
}

func (rf *Raft) replicateLogSingle(server int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.mu.Lock()
	state := rf.state
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[server]:]))
	copy(args.Entries, rf.log[rf.nextIndex[server]:])
	args.LeaderCommit = rf.commitIndex
	leaderTerm := rf.currentTerm
	toCommitIdx := len(rf.log) - 1
	toNextIdx := len(rf.log)
	// fmt.Printf("%d to %d: ", rf.me, server)
	// fmt.Println(args.Entries)
	rf.mu.Unlock()

	// 只要rf依然是leader，就不断重发该请求，直到请求成功
	for state == 2 {
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if ok {
			// 先进行任期判断，注意应使用发送request时候的任期
			if leaderTerm < reply.Term {
				// 如果对方任期更新，退出循环
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					// 如果leader的当前任期过期，更新rf的任期，转换为follower
					rf.currentTerm = reply.Term
					rf.setFollower()
				}

				rf.mu.Unlock()
				break
			}

			if reply.Success {
				// 同步log成功，更新leader的nextIndex[]和matchIndex[]，退出循环
				rf.mu.Lock()
				if toNextIdx > rf.nextIndex[server] {
					// 仅单调递增
					rf.nextIndex[server] = toNextIdx
				}
				if toCommitIdx > rf.matchIndex[server] {
					// 仅单调递增
					rf.matchIndex[server] = toCommitIdx
				}
				rf.mu.Unlock()
				break
			} else {
				// 同步log失败，将PrevLogIndex前移一位重试
				rf.mu.Lock()
				insertLog := LogEntry{}
				insertLog.Command = rf.log[args.PrevLogIndex].Command
				insertLog.Term = rf.log[args.PrevLogIndex].Term
				args.Entries = prependEntry(args.Entries, insertLog)
				args.PrevLogIndex--
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				rf.mu.Unlock()
			}
		}
		// 更新状态后尝试重试
		rf.mu.Lock()
		state = rf.state
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// 向所有followers同步command
func (rf *Raft) replicateLog(cmd interface{}) int {
	rf.mu.Lock()
	// local log添加cmd
	rf.log = append(rf.log, LogEntry{cmd, rf.currentTerm})
	// 日志最终commit时的索引
	toCommitIdx := len(rf.log) - 1
	rf.mu.Unlock()

	for idx := range rf.peers {
		// 按idx给其他服务器发送同步请求
		if idx == rf.me {
			// 自己就不用发了
			continue
		}

		// 给单个服务器发送log同步的协程
		go rf.replicateLogSingle(idx)
	}

	return toCommitIdx
}

func (rf *Raft) commitLog() {
	// leader提交日志协程，只要peer还是leader，就会不断循环下去
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()

	for state == 2 {
		rf.mu.Lock()
		// 找到最大的大多数值
		matchMajority := rf.findMatchMajority()
		if matchMajority > rf.commitIndex && rf.log[matchMajority].Term == rf.currentTerm {
			rf.commitIndex = matchMajority
		}
		state = rf.state
		rf.mu.Unlock()

		// time sleep一会儿
		time.Sleep(10 * time.Millisecond)
	}
}

type ValueCount struct {
	value int
	count int
}

func (rf *Raft) findMatchMajority() int {
	max := 0
	for _, v := range rf.matchIndex {
		if v > max {
			max = v
		}
	}

	tmp := make([]int, max+1)
	for _, v := range rf.matchIndex {
		tmp[v] += 1
	}

	var array []ValueCount
	for idx, v := range tmp {
		if v > 0 {
			array = append(array, ValueCount{idx, v})
		}
	}

	for idx, v := range array {
		for i := 0; i < idx; i++ {
			array[i].count += v.count
		}
	}

	majority := len(rf.peers) / 2
	for i := len(array) - 1; i >= 0; i-- {
		if array[i].count >= majority {
			return array[i].value
		}
	}

	return 0
}

func (rf *Raft) heartBeats() {
	// leader的心跳协程，只要peer还是leader，就会不断循环下去
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	for state == 2 {
		// 先把自己的选举timer刷新一下
		rf.mu.Lock()
		state = rf.state
		rf.flushElectionTimer()
		rf.mu.Unlock()
		for idx := range rf.peers {
			// 按idx给其他服务器发送心跳
			if idx == rf.me {
				continue
			}

			// 给单个服务器发送心跳的协程
			go rf.heartBeatsSingle(idx)
		}

		time.Sleep(100 * time.Millisecond) // 100ms一次，这样1秒就是10次内，符合规定
	}
}

func (rf *Raft) heartBeatsSingle(server int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.Entries = make([]LogEntry, 0) // 心跳请求，log为空数组
	args.LeaderCommit = rf.commitIndex
	state := rf.state
	rf.mu.Unlock()

	if state != 2 {
		// 不是leader了，直接返回
		return
	}
	// 发出心跳请求，无论是否成功，都继续，不需要重试
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		// 对返回的响应做判断
		// 判断响应的term
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		if currentTerm < reply.Term {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.setFollower()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) getVoteSingle(server int, ch chan<- bool) {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	rf.mu.Lock()
	// 获取刷新时间和超时时间
	// 这两个时间在该协程内应该是不变的
	flushtime := rf.lastFlushTime
	electtionTimeOut := rf.electionTimeOut
	state := rf.state
	rf.mu.Unlock()

	var currentTime int64
	for state == 1 {
		currentTime = time.Now().UnixMilli()
		if (currentTime - flushtime) > electtionTimeOut {
			// 超时，退出投票协程
			break
		}

		rf.mu.Lock()
		state = rf.state
		// args参数赋值
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
		if ok {
			rf.mu.Lock()
			// 判断响应的term
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.setFollower()
			}
			rf.mu.Unlock()
			ch <- reply.VoteGranted
			break
		}
		// 如果不ok，就会一直重试直到超时
	}
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
	term, isLeader = rf.GetState()
	if !isLeader {
		// 非leader，直接返回
		return index, term, isLeader
	}

	// 该命令会立即返回，非阻塞
	index = rf.replicateLog(command)
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

func (rf *Raft) flushElectionTimer() {
	rf.electionTimeOut = int64(rand.Intn(1000-500) + 500) // 选举超时设为500-1000ms之间的随机值
	rf.lastFlushTime = time.Now().UnixMilli()             // 当前时间设为刷新时间
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		currentTime := time.Now().UnixMilli()
		flushTime := rf.lastFlushTime
		electionTimeOut := rf.electionTimeOut
		rf.mu.Unlock()
		// 判断是否应该发起选举，如果最近刷新过选举timer，则跳过
		if currentTime-flushTime > electionTimeOut {
			// election timeout超时，开始选举
			rf.mu.Lock()
			rf.state = 1                        // 状态转化为candidate
			rf.currentTerm = rf.currentTerm + 1 // 任期加一
			rf.votedFor = rf.me                 // 投自己
			rf.flushElectionTimer()             // 刷新选举timer
			rf.mu.Unlock()

			// 向其他server发送投票请求
			chVotes := make(chan bool, len(rf.peers)) // buffer设置为server数
			chVotes <- true                           // 先给自己来个yes
			for idx := range rf.peers {
				if rf.me == idx {
					continue
				}
				// fmt.Printf("%d to %d req single\n", rf.me, idx)
				go rf.getVoteSingle(idx, chVotes)
			}

			// 获取选举timer，在收集投票循环中不应该再更新，不然一直无法超时退出
			rf.mu.Lock()
			flushTime = rf.lastFlushTime
			electionTimeOut = rf.electionTimeOut
			rf.mu.Unlock()

			// 收集投票
			votes := 0
			var state int
			for {
				currentTime = time.Now().UnixMilli()

				// 判断超时或成为leader或有其他peer成为了leader
				// 1. 当前peer已经转换为follower
				// 当接收到其他peer的appendentries RPC时，表示其他peer已经成为leader，将转化为follower
				// 当接收到其他peer的RPC中包含的term更新时，也会转化为follower
				rf.mu.Lock()
				state = rf.state
				rf.mu.Unlock()
				if state == 0 {
					break
				}
				// 2. 收集到超过一半的投票，成为leader
				if votes >= (len(rf.peers)/2 + 1) {
					rf.mu.Lock()
					// fmt.Printf("%d: 当选！\n", rf.me)
					// state设置为leader即2
					rf.state = 2
					// 初始化各种状态
					rf.nextIndex = nil
					rf.matchIndex = nil
					for i := 0; i < len(rf.peers); i++ {
						// 初始化为leader的last log index + 1
						rf.nextIndex = append(rf.nextIndex, len(rf.log))
						// 初始化为0
						rf.matchIndex = append(rf.matchIndex, 0)
					}
					rf.mu.Unlock()
					// 开始干leader的活了
					// 心跳协程走起
					go rf.heartBeats()
					// 提交日志协程走起
					go rf.commitLog()
					break
				}
				// 3. 超时，退出循环，理论上会再次进入该选举投票循环
				if currentTime-flushTime > electionTimeOut {
					break
				}

				select {
				case v := <-chVotes:
					// 通道中有投票信息，按照结果统计
					if v {
						// 如果投票结果v为true，加1分
						votes += 1
					}
				default:
					// 通道中没有投票信息，10ms等待后重新尝试获取
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
		time.Sleep(10 * time.Millisecond) // 10ms等待后重试
	}
}

func (rf *Raft) checkApply() {
	for {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
		}
		rf.mu.Unlock()

		// time sleep
		time.Sleep(10 * time.Millisecond)
	}
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
	// 初始化rf值
	// 这部分尚没有goroutine并发访问，不需要加锁
	rf.currentTerm = 0
	rf.setFollower()
	rf.flushElectionTimer()
	rf.log = append(rf.log, LogEntry{Command: 0, Term: rf.currentTerm})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// apply committed log
	go rf.checkApply()

	return rf
}
