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

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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

type RaftState int

const (
	FOLLOWER   RaftState = 0
	CANDIDATOR RaftState = 1
	LEADER     RaftState = 2
)

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
	timeoutHeartBeat int         // 心跳频率ms
	timeoutElect     int         // 选举频率ms
	timerHeartBeat   *time.Timer // 心跳计时器
	timerElect       *time.Timer // 选举计时器

	// 当前角色状态
	role RaftState
	// apply通道
	applyCh   chan ApplyMsg
	applyCond *sync.Cond // apply时的条件变量
	// 快照状态
	snapshotState     []byte
	lastIncludedIndex int
	lastIncludedTerm  int
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
	isleader = (rf.role == LEADER)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// 持久化state和snapshot到persister
func (rf *Raft) persistStateAndSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()

	// snapshot data
	sw := new(bytes.Buffer)
	se := labgob.NewEncoder(sw)
	se.Encode(rf.snapshotState)
	sdata := sw.Bytes()
	rf.persister.SaveStateAndSnapshot(data, sdata)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("decode error......")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshotState []byte
	if d.Decode(&snapshotState) != nil {
		fmt.Println("decode error......")
	} else {
		rf.snapshotState = snapshotState
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	DPrintf("%d cond install snapshot\n", rf.me)

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex >= lastIncludedIndex {
		// refuse过期的snapshot
		DPrintf("%d 过期snapshot, commit: %d, lastIncludedIndex: %d\n", rf.me, rf.commitIndex, lastIncludedIndex)
		return false
	}

	if rf.logIdxToAbsIdx(len(rf.log)-1) < lastIncludedIndex {
		// 丢弃整个log
		DPrintf("%d 丢掉整个log\n", rf.me)
		rf.log = rf.log[:1]
	} else {
		// 保留超过lastIncludedIndex之后的log
		// 这里的lastIncludeIndex为绝对index，减去rf的lastIncludedIndex方为log切片中的index
		rf.log = append(rf.log[:1], rf.log[rf.absIdxToLogIdx(lastIncludedIndex+1):]...)
		DPrintf("%d 保留snapshot之后的log\n", rf.me)
	}

	rf.snapshotState = snapshot
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	// 这一步很重要，当log为空时(只有初始化的一条)，log[0]负责保存已安装的snapshot的term
	rf.log[0].Term = lastIncludedTerm

	// 持久化state和snapshot
	rf.persistStateAndSnapshot()
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("%d make snapshot at index %d\n", rf.me, index)
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	idxLog := rf.log[rf.absIdxToLogIdx(index)]
	// 截断log，仅保留index之后的log
	// 这里的index为绝对index，减去rf的lastIncludedIndex方为log切片中的index
	rf.log = append(rf.log[:1], rf.log[rf.absIdxToLogIdx(index+1):]...)
	rf.snapshotState = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = idxLog.Term
	// 这一步很重要，当log为空时(只有初始化的一条)，log[0]负责保存已安装的snapshot的term
	rf.log[0].Term = rf.lastIncludedTerm

	// 只有已经被commit和apply的msg才能被snapshot，因此这里不需要更新peer的commitIndex和lastApplied
	// 持久化state和snapshot
	rf.persistStateAndSnapshot()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("%d install snapshot\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() {
		reply.Term = rf.currentTerm
	}()

	DPrintf("installsnapshot==>%d to %d: my last apply %d, my log length %d; my commit %d, my term %d, leader term %d; peer lastIncludedIndex %d, peer lastIncludedTerm %d; leader lastIncludedIndex %d, leader lastIncludedTerm %d\n",
		args.LeaderId, rf.me, rf.lastApplied, len(rf.log), rf.commitIndex, rf.currentTerm, args.Term, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm)

	if args.Term < rf.currentTerm {
		// 对方term过期，拒绝，直接返回
		return
	}

	if args.Term > rf.currentTerm {
		// 如果对方term更新，更新到该任期，转换为follower
		rf.currentTerm = args.Term
		rf.toFollower()
	}

	// 刷新时间
	rf.flushElectionTimer()

	if args.LastIncludedIndex <= rf.commitIndex {
		// 快照过期，不推入channel直接返回
		return
	}

	// 将待install的snapshot推入apply channel
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.applyCh <- ApplyMsg{CommandValid: false, SnapshotValid: true, SnapshotTerm: args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex, Snapshot: args.Data}
	}()
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
		rf.toFollower()
		rf.persist()
	}

	if rf.currentTerm > args.Term {
		// peer的term更优，投反对票，提前退出
		reply.VoteGranted = false
		return
	}

	if (rf.votedFor == -1) || (rf.votedFor == args.CandidateId) {
		// peer尚未投票或投票对象为对方
		// candidate或leader状态，peer的投票状态应该都为自己，不会进入该判断
		if (args.LastLogTerm > rf.lastLog().Term) ||
			((args.LastLogTerm == rf.lastLog().Term) && (args.LastLogIndex >= rf.logIdxToAbsIdx(len(rf.log)-1))) {
			// 对方的log任期更新，或对方的log任期相同且log index大于等于peer
			rf.votedFor = args.CandidateId
			rf.persist()
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

func (rf *Raft) toFollower() {
	DPrintf("%d 贬为庶人...\n", rf.me)
	rf.role = FOLLOWER
	rf.votedFor = -1
}

func (rf *Raft) toLeader() {
	DPrintf("%d 神选!!\n", rf.me)
	// role设置为leader
	rf.role = LEADER
	// 初始化各种状态
	rf.nextIndex = nil
	rf.matchIndex = nil
	for i := 0; i < len(rf.peers); i++ {
		// 初始化为leader的last log index + 1，这里的索引为绝对索引
		rf.nextIndex = append(rf.nextIndex, rf.logIdxToAbsIdx(len(rf.log)))
		// 初始化为0
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// 开始干leader的活了
	// 提交日志协程走起
	go rf.commitLog()
	// 立即发起一轮广播，通过让心跳超时触发
	rf.timerHeartBeat.Reset(0)
}

func (rf *Raft) toCandidator() {
	DPrintf("%d 升格为候选神\n", rf.me)
	rf.role = CANDIDATOR                // 状态转化为candidate
	rf.currentTerm = rf.currentTerm + 1 // 任期加一
	rf.votedFor = rf.me                 // 投自己
	rf.flushElectionTimer()             // 刷新选举timer
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() {
		reply.Term = rf.currentTerm
	}()

	// 设置回复的默认值
	reply.Success = true

	if rf.currentTerm <= args.Term {
		// 如果对方term未过期，刷新选举timer
		rf.flushElectionTimer()

		if rf.currentTerm < args.Term {
			// 如果对方的term更优，将peer的term更新到对方的term，同时转换状态为follower
			rf.currentTerm = args.Term
			rf.toFollower()
		}
	}

	DPrintf("appendentries==>%d to %d: my last apply %d, my log length %d, received entries length %d; my commit %d, leader commit %d; my term %d, leader term %d; peer lastIncludedIndex %d, peer lastIncludedTerm %d; pidx %d, pt %d\n",
		args.LeaderId, rf.me, rf.lastApplied, len(rf.log), len(args.Entries), rf.commitIndex, args.LeaderCommit, rf.currentTerm, args.Term, rf.lastIncludedIndex, rf.lastIncludedTerm, args.PrevLogIndex, args.PrevLogTerm)

	if args.PrevLogIndex < rf.lastIncludedIndex {
		DPrintf("pIdx小于peer lastIncludedIndex\n")
		return
	}

	// 下面都是append entries的判断
	if args.Term < rf.currentTerm {
		// leader的任期过期，拒绝
		DPrintf("leader过期，拒绝\n")
		reply.Success = false
		return
	}

	if rf.logIdxToAbsIdx(len(rf.log)-1) < args.PrevLogIndex {
		// PrevLogIndex超过peer的log索引上限，拒绝
		DPrintf("pIdx越界，拒绝\n")
		reply.Success = false
		return
	}

	if rf.absIdxToLogIdx(args.PrevLogIndex) >= 0 && rf.log[rf.absIdxToLogIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// 出现冲突，拒绝
		DPrintf("term冲突，拒绝，pIdx term %d, leader term %d\n", rf.log[rf.absIdxToLogIdx(args.PrevLogIndex)].Term, args.PrevLogTerm)
		reply.Success = false
		return
	}

	// 如果以上检查都通过，进入追加entries阶段
	// 在peer的log中写入参数中的entries
	// 这里需要满足幂等性，已经存在的entry将跳过
	// 冲突的entry，将删除peer的对应entry及其随后entries
	// 空entries，比如心跳请求，将跳过
	log_idx := -1
	for idx := range args.Entries {
		log_idx = rf.absIdxToLogIdx(args.PrevLogIndex + idx + 1)
		if log_idx >= len(rf.log) {
			// 该idx位置开始的entries为新增entries，追加到peer的log后
			DPrintf("追加数据\n")
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}

		if args.Entries[idx].Term != rf.log[log_idx].Term {
			// 冲突发生了
			DPrintf("擦除冲突数据\n")
			rf.log = rf.log[:log_idx]
			// 再将后续entries追加
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}

	// 判断peer的commit是否需要更新
	if args.LeaderCommit < rf.logIdxToAbsIdx(len(rf.log)) && args.LeaderCommit > rf.commitIndex {
		// 更新peer的commitIndex
		if args.LeaderCommit < args.PrevLogIndex+len(args.Entries) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}
		DPrintf("%d commit to %d\n", rf.me, rf.commitIndex)
		// 通知apply协程
		rf.applyCond.Signal()
	} else {
		DPrintf("%d cannot commit, leader commit %d, peer last log index %d, peer commit %d\n",
			rf.me, args.LeaderCommit, rf.logIdxToAbsIdx(len(rf.log)-1), rf.commitIndex)
	}
}

// 向所有followers同步command
func (rf *Raft) replicateLog(cmd interface{}) int {
	rf.mu.Lock()
	// local log添加cmd
	rf.log = append(rf.log, LogEntry{cmd, rf.currentTerm})
	rf.persist()
	// 日志最终commit时的索引，为绝对索引
	toCommitIdx := rf.logIdxToAbsIdx(len(rf.log) - 1)
	rf.mu.Unlock()

	// 立马开始一轮广播
	go rf.broadCast()
	return toCommitIdx
}

func (rf *Raft) commitLog() {
	// leader提交日志协程，只要peer还是leader，就会不断循环下去
	for {
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		// 找到最大的大多数值
		matchMajority := rf.findMatchMajority()
		if matchMajority > rf.commitIndex && rf.log[rf.absIdxToLogIdx(matchMajority)].Term == rf.currentTerm {
			rf.commitIndex = matchMajority
			// 通知apply协程
			rf.applyCond.Signal()
		}
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

func (rf *Raft) broadCast() {
	// 广播给所有的server信息
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		go rf.AppendEntriesWrap(idx)
	}
}

func (rf *Raft) AppendEntriesWrap(server int) {

	// 参数赋值
	rf.mu.Lock()
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		// install snapshot
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshotState,
		}
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()
		// 发出rpc
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != LEADER || args.Term != rf.currentTerm {
			return
		}
		if !ok {
			return
		}
		rf.adjustReplyTerm(reply.Term)
		if rf.role == FOLLOWER {
			// 对方term更新
			return
		}

		// 只要rpc发送成功，说明对方一定会更新到比快照更新的状态
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	} else {
		// normal append entries
		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.log[rf.absIdxToLogIdx(args.PrevLogIndex)].Term
		args.Entries = make([]LogEntry, len(rf.log[rf.absIdxToLogIdx(rf.nextIndex[server]):]))
		copy(args.Entries, rf.log[rf.absIdxToLogIdx(rf.nextIndex[server]):])
		args.LeaderCommit = rf.commitIndex
		DPrintf("%d send to %d: pIdx %d, sender lastIncludedIndex %d\n", rf.me, server, args.PrevLogIndex, rf.lastIncludedIndex)
		rf.mu.Unlock()
		// 发出rpc
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()
		if rf.role != LEADER || args.Term != rf.currentTerm {
			// 过期请求，直接返回
			return
		}
		if !ok {
			// 请求失败，直接返回
			return
		}
		rf.adjustReplyTerm(reply.Term)
		if rf.role == FOLLOWER {
			// 对方term更新
			return
		}

		if !reply.Success {
			// 响应Success为false
			rf.onLogReplyFail(args.PrevLogIndex, args.PrevLogTerm, server)
		} else {
			rf.onLogReplySucceed(args.PrevLogIndex+len(args.Entries), server)
		}
	}
}

func (rf *Raft) onLogReplySucceed(matchIndex int, server int) {
	rf.matchIndex[server] = matchIndex
	rf.nextIndex[server] = matchIndex + 1
}

func (rf *Raft) onLogReplyFail(prevLogIndex int, prevLogTerm int, server int) {
	DPrintf("%d append log fail, peer lastIncludedIdx %d, adjust nextIndex[%d], pIdx %d, pt %d, previous nextIdx %d\n", rf.me, rf.lastIncludedIndex, server, prevLogIndex, prevLogTerm, rf.nextIndex[server])
	for {
		if rf.absIdxToLogIdx((prevLogIndex)) < 0 {
			// 越界
			break
		}

		if prevLogTerm != rf.log[rf.absIdxToLogIdx(prevLogIndex)].Term {
			// 到达前一term
			break
		}
		// PrevLogIndex不断递减直到前一term
		prevLogIndex--
	}
	rf.nextIndex[server] = prevLogIndex + 1
}

func (rf *Raft) GetVoteWrap(server int, ch chan<- bool) {
	// 处理向单个follower发起投票请求的函数
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}

	rf.mu.Lock()
	// args参数赋值
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	// 绝对索引
	args.LastLogIndex = rf.logIdxToAbsIdx(len(rf.log) - 1)
	args.LastLogTerm = rf.lastLog().Term
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if ok {
		rf.mu.Lock()
		rf.adjustReplyTerm(reply.Term)
		rf.mu.Unlock()
		ch <- reply.VoteGranted
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
	timeout := rand.Intn(400) + rf.timeoutElect // 500-900ms的选举超时时间
	rf.timerElect.Reset(time.Duration(timeout) * time.Millisecond)
}

func (rf *Raft) flushHeartBeatTimer() {
	rf.timerHeartBeat.Reset(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.toCandidator()
	rf.persist()
	rf.mu.Unlock()

	// 向其他server发送投票请求
	chVotes := make(chan bool, len(rf.peers)) // buffer设置为server数
	chVotes <- true                           // 先给自己来个yes
	for idx := range rf.peers {
		if rf.me == idx {
			continue
		}
		// fmt.Printf("%d to %d req single\n", rf.me, idx)
		go rf.GetVoteWrap(idx, chVotes)
	}

	// 收集投票
	votes := 0
	for {
		// 1. 当前peer已经转换为follower
		// 当接收到其他peer的appendentries RPC时，表示其他peer已经成为leader，将转化为follower
		// 当接收到其他peer的RPC中包含的term更新时，也会转化为follower
		rf.mu.Lock()
		if rf.role != CANDIDATOR {
			rf.mu.Unlock()
			break
		}
		// 2. 收集到超过一半的投票，成为leader
		if votes >= (len(rf.peers)/2 + 1) {
			rf.toLeader()
			rf.mu.Unlock()
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
		rf.mu.Unlock()
		DPrintf("%d 收集到投票%d/%d\n", rf.me, votes, len(rf.peers))
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.timerElect.C:
			rf.mu.Lock()
			if rf.role != LEADER {
				go rf.startElection()
			}
			rf.flushElectionTimer()
			rf.mu.Unlock()

		case <-rf.timerHeartBeat.C:
			rf.mu.Lock()
			if rf.role == LEADER {
				go rf.broadCast()
			}
			rf.flushHeartBeatTimer()
			rf.mu.Unlock()
			// 如果没有case满足，他将阻塞直到有通道有信息
		}
	}
}

func (rf *Raft) checkApply() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex {
			// 不符合apply条件，放弃锁并等待signal唤醒
			rf.applyCond.Wait()
		}

		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		applyEntries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
		// applyStartIdx := rf.absIdxToLogIdx(rf.lastApplied + 1)
		// applyEndIdx := rf.absIdxToLogIdx(rf.commitIndex + 1)
		DPrintf("%d apply entries startIdx %d to endIdx %d, peer lastIncludedIndex %d\n",
			rf.me, rf.absIdxToLogIdx(rf.lastApplied+1), rf.absIdxToLogIdx(rf.commitIndex+1),
			rf.lastIncludedIndex)
		copy(applyEntries, rf.log[rf.absIdxToLogIdx(rf.lastApplied+1):rf.absIdxToLogIdx(rf.commitIndex+1)])
		rf.mu.Unlock()

		for idx, entry := range applyEntries {
			command := entry.Command
			commandIdx := lastApplied + 1 + idx
			DPrintf("%d apply command index: %d\n", rf.me, commandIdx)
			// apply时需要解锁
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: command, CommandIndex: commandIdx}
		}

		rf.mu.Lock()
		if commitIndex > rf.lastApplied {
			// 由于解锁过程中，lastApplied可能更新至更新，比如安装一个新的snapshot
			// 因此需要同原commitIndex比较
			DPrintf("%d update last applied %d to %d\n", rf.me, rf.lastApplied, commitIndex)
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) adjustReplyTerm(replyTerm int) {
	// 判断响应的term是否更新
	if rf.currentTerm < replyTerm {
		rf.currentTerm = replyTerm
		rf.toFollower()
		rf.persist()
	}
}

func (rf *Raft) logIdxToAbsIdx(logIdx int) int {
	// 日志索引向绝对索引的转化
	return logIdx + rf.lastIncludedIndex
}

func (rf *Raft) absIdxToLogIdx(absIdx int) int {
	// 绝对索引向日志索引的转化
	return absIdx - rf.lastIncludedIndex
}

func (rf *Raft) lastLog() LogEntry {
	// 获取最后一条日志
	return rf.log[len(rf.log)-1]
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
	rf.toFollower()
	rf.log = append(rf.log, LogEntry{Command: 0, Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.timeoutHeartBeat = 100 // 100ms
	rf.timeoutElect = 500     // 500ms
	rf.timerHeartBeat = time.NewTimer(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
	rf.timerElect = time.NewTimer(time.Duration(rf.timeoutElect) * time.Millisecond)
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	// apply committed log
	go rf.checkApply()

	return rf
}
