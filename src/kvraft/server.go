package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string
	ClientId  int
	CommandId int
}

type ReplyMsg struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database   map[string]string
	lastOpMap  map[int]int           // 对应client已被大多数服务器处理的日志id
	replyChMap map[int]chan ReplyMsg // 每条日志id对应的响应channel
}

func (kv *KVServer) GetValGR(key string, reply *GetReply) {
	if val, ok := kv.database[key]; ok {
		// key存在
		reply.Err = OK
		reply.Value = val
	} else {
		// key不存在
		reply.Err = ErrNoKey
		reply.Value = ""
	}
}

func (kv *KVServer) GetValRM(key string, reply *ReplyMsg) {
	if val, ok := kv.database[key]; ok {
		// key存在
		reply.Err = OK
		reply.Value = val
	} else {
		// key不存在
		reply.Err = ErrNoKey
		reply.Value = ""
	}
}

func (kv *KVServer) freeReplyMap(index int) {
	kv.mu.Lock()
	delete(kv.replyChMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// 判断该op是否已经commit到raft log
	if lastOpIdx, ok := kv.lastOpMap[args.ClientId]; ok {
		if lastOpIdx >= args.CommandId {
			// op索引已过期，直接取值返回
			kv.GetValGR(args.Key, reply)
			kv.mu.Unlock()
			DPrintf("%d get return early, lastOpIdx %d, command id %d", kv.me, lastOpIdx, args.CommandId)
			return
		}
	}

	// 向raft log发送op request
	op := Op{
		Key:       args.Key,
		Value:     "",
		Op:        "Get",
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	toCommitIdx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		kv.mu.Unlock()
		return
	}

	// 创建toCommitIdx对应的响应channel
	replyCh := make(chan ReplyMsg)
	kv.replyChMap[toCommitIdx] = replyCh
	kv.mu.Unlock()

	// 等待execMsg后的响应
	select {
	case replyMsg := <-replyCh:
		reply.Err = replyMsg.Err
		reply.Value = replyMsg.Value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	go kv.freeReplyMap(toCommitIdx)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// 判断该op是否已经commit到raft log
	if lastOpIdx, ok := kv.lastOpMap[args.ClientId]; ok {
		if lastOpIdx >= args.CommandId {
			// op索引已过期，直接返回，避免op重复执行
			reply.Err = OK
			kv.mu.Unlock()
			DPrintf("%d put append return early, lastOpIdx %d, command id %d", kv.me, lastOpIdx, args.CommandId)
			return
		}
	}

	// 向raft log发送op request
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	toCommitIdx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// 创建toCommitIdx对应的响应channel
	replyCh := make(chan ReplyMsg)
	kv.replyChMap[toCommitIdx] = replyCh
	kv.mu.Unlock()

	// 等待execMsg后的响应
	select {
	case replyMsg := <-replyCh:
		reply.Err = replyMsg.Err
	case <-time.After(500 * time.Millisecond):
		DPrintf("%d wait reply timeout", kv.me)
		reply.Err = ErrTimeOut
	}

	go kv.freeReplyMap(toCommitIdx)
}

func (kv *KVServer) overThreshold() bool {
	// maxraftstate为-1说明不做限制，永远不会到达阈值
	if kv.maxraftstate == -1 {
		return false
	}

	// 90% maxraftstate设定为阈值
	var prop float32
	prop = float32(kv.rf.GetRaftStateSize()) / float32(kv.maxraftstate)
	DPrintf("%d prop %f", kv.me, prop)
	return prop >= 0.9
}

func (kv *KVServer) doKVSnapshot(index int) {
	// kv database
	// lastOpMap
	// 需要做snapshot保存快照
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	err := e.Encode(kv.database)
	if err != nil {
		DPrintf("%d encode err %s", kv.me, err)
		return
	}

	err = e.Encode(kv.lastOpMap)
	if err != nil {
		DPrintf("%d encode err %s", kv.me, err)
		return
	}

	data := b.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) loadKVSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshot := kv.rf.GetSnapshot()
	DPrintf("%d install snapshot, length %d", kv.me, len(snapshot))

	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	b := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(b)
	var database map[string]string
	var lastOpMap map[int]int
	err := d.Decode(&database)
	if err != nil {
		DPrintf("%d decode error %s", kv.me, err)
		DPrintf(string(snapshot))
	} else {
		kv.database = database
	}

	err = d.Decode(&lastOpMap)
	if err != nil {
		DPrintf("%d decode error %s", kv.me, err)
	} else {
		kv.lastOpMap = lastOpMap
	}
}

func (kv *KVServer) applyOp(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := msg.Command.(Op)
	commitIdx := msg.CommandIndex
	replyTerm := msg.CommandTerm

	DPrintf("%d op %s, clientId %d, commandId %d, commitIdx %d, term %d, key %s, value %s", kv.me, op.Op, op.ClientId, op.CommandId, commitIdx, replyTerm, op.Key, op.Value)
	var replyMsg ReplyMsg

	// 判断op是否过期
	if lastOpIdx, ok := kv.lastOpMap[op.ClientId]; ok {
		if lastOpIdx >= op.CommandId {
			// 过期，放任超时
			DPrintf("%d stale op, lastOpIdx %d, client id %d, command id %d", kv.me, lastOpIdx, op.ClientId, op.CommandId)
			return
		}
	}

	// op为全新op，执行
	if op.Op == "Get" {
		// 读取Key，不影响database
		kv.GetValRM(op.Key, &replyMsg)
	} else if op.Op == "Put" {
		// 直接设置key
		kv.database[op.Key] = op.Value
		replyMsg = ReplyMsg{OK, ""}
	} else {
		// append
		if oldVal, ok := kv.database[op.Key]; ok {
			// key存在，append
			kv.database[op.Key] = oldVal + op.Value
		} else {
			// key不存在，replace
			kv.database[op.Key] = op.Value
		}
		replyMsg = ReplyMsg{OK, ""}
	}

	// 判断term是否过期
	term, isLeader := kv.rf.GetState()
	// 如果exec时的当前term跟op提交时一致，并且kv依然是leader，则成功响应，否则放任超时
	if replyCh, ok := kv.replyChMap[commitIdx]; ok && isLeader && term == replyTerm {
		replyCh <- replyMsg
	}
	// 更新对应client的最新op id
	kv.lastOpMap[op.ClientId] = op.CommandId
	// 判断是否需要snapshot
	if kv.overThreshold() {
		kv.doKVSnapshot(commitIdx)
	}
}

func (kv *KVServer) applySnapshot(msg raft.ApplyMsg) {
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		// raft协议层安装snapshot成功，kv service层再加载其state状态
		DPrintf("%d cond install succeed in kv layer, loadKVSnapshot then", kv.me)
		kv.loadKVSnapshot()
	}
	// 无需通过channel做任何通知
}

func (kv *KVServer) execMsg() {
	// 逐个应用raft已commit的op
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			// 从applyCh中获取已提交的msg
			if msg.CommandValid {
				kv.applyOp(msg)
			} else if msg.SnapshotValid {
				kv.applySnapshot(msg)
			} else {
				DPrintf("%d error applied msg", kv.me)
				kv.Kill()
			}
		}
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.database = make(map[string]string)
	kv.lastOpMap = make(map[int]int)
	kv.replyChMap = make(map[int]chan ReplyMsg)
	// 加载缓存snapshot，用于reboot后的状态恢复
	DPrintf("%d state restore after booting", kv.me)
	kv.loadKVSnapshot()

	// 起一个携程，不断执行从raft log commit的op或snapshot
	go kv.execMsg()

	return kv
}
