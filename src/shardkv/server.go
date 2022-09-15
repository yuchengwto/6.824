package shardkv

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = true

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
	DataBatch map[string]map[int]string
	Op        string
	ShardId   int
	ConfigNum int
	Config    shardctrler.Config
	LastOpMap map[int]map[int]int
	ClientId  int
	CommandId int
}

type ShardTransferArgs struct {
	ShardId   int
	GID       int
	ConfigNum int
	BEGIN     bool
	END       bool
}

type ShardTransferReply struct {
	Err         Err
	ShardData   map[string]map[int]string
	ShardLastOp map[int]int
}

type ReplyMsg struct {
	Err         Err
	Value       string
	ShardData   map[string]map[int]string
	ShardLastOp map[int]int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// shardId -> subdatabase; key -> { commandId -> value }	commandId作为版本号，最新一条的id为-1
	database   map[int]map[string]map[int]string
	lastOpMap  map[int]map[int]int
	replyChMap map[int]chan ReplyMsg
	dead       int32
	mck        *shardctrler.Clerk
	config     shardctrler.Config
	// 是否正在移动分片
	sharding int32
	// 是否处于分片传输过程中
	transferring int32
}

func (kv *ShardKV) checkShard(shardId int) bool {
	return kv.config.Shards[shardId] == kv.gid
}

func (kv *ShardKV) checkConfigNumMatch(configNum int) bool {
	return kv.config.Num == configNum
}

func (kv *ShardKV) freeReplyMap(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.replyChMap, index)
}

func (kv *ShardKV) getByKeyAndId(key string, id int, shardId int) string {
	if subdb, ok := kv.database[shardId]; ok {
		if pair, ok := subdb[key]; ok {
			// key存在
			if val, ok := pair[id]; ok {
				return val
			} else {
				DPrintf("%d of gid %d key %s, id %d NOT EXIST", kv.me, kv.gid, key, id)
			}
		} else {
			DPrintf("%d of gid %d key %s NOT EXIST", kv.me, kv.gid, key)
		}
	} else {
		DPrintf("%d of gid %d shard %d NOT EXIST", kv.me, kv.gid, shardId)
	}

	return ""
}

func (kv *ShardKV) getByKeyThenUpdate(key string, id int, shardId int) string {
	if subdb, ok := kv.database[shardId]; ok {
		if pair, ok := subdb[key]; ok {
			// key存在
			if val, ok := pair[-1]; ok {
				pair[id] = val
				return val
			} else {
				DPrintf("%d of gid %d key %s, id %d NOT EXIST", kv.me, kv.gid, key, id)
			}
		} else {
			DPrintf("%d of gid %d key %s NOT EXIST", kv.me, kv.gid, key)
		}
	} else {
		DPrintf("%d of gid %d shard %d NOT EXIST", kv.me, kv.gid, shardId)
	}

	return ""
}

func (kv *ShardKV) setByKeyAndId(key string, id int, shardId int, val string) {
	if _, ok := kv.database[shardId]; !ok {
		// shardId不存在
		// 创建一个新的空的
		kv.database[shardId] = make(map[string]map[int]string)
	}

	if pair, ok := kv.database[shardId][key]; ok {
		// key存在
		pair[id] = val
		if len(val) > len(pair[-1]) {
			pair[-1] = val
		}
	} else {
		// key不存在
		kv.database[shardId][key] = make(map[int]string)
		kv.database[shardId][key][id] = val
		if len(val) > len(kv.database[shardId][key][-1]) {
			kv.database[shardId][key][-1] = val
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.checkConfigNumMatch(args.ConfigNum) {
		// server的版本号过低，超时退出
		reply.Err = ErrTimeOut
		DPrintf("%d of gid %d config number %d not match request %d in GET, refuse", kv.me, kv.gid, kv.config.Num, args.ConfigNum)
		return
	}

	if atomic.LoadInt32(&kv.sharding) == 1 {
		// 正在分片中，直接超时返回
		reply.Err = ErrTimeOut
		DPrintf("%d of gid %d is sharding, refuse any outer requests", kv.me, kv.gid)
		return
	}

	// 判断该request是否属于group拥有shard
	if !kv.checkShard(args.ShardId) {
		DPrintf("%d of gid %d wrong group in get, input shard %d", kv.me, kv.gid, args.ShardId)
		reply.Err = ErrWrongGroup
		reply.Value = ""
		return
	}

	// 判断该op是否已经commit到raft log
	if lastOpIdx, ok := kv.lastOpMap[args.ShardId][args.ClientId]; ok {
		if lastOpIdx >= args.CommandId {
			// op索引已过期，直接取值返回
			DPrintf("%d return early in Get, lastOpIdx %d, clientId %d, commandId %d, key %s", kv.me, lastOpIdx, args.ClientId, args.CommandId, args.Key)
			val := kv.getByKeyAndId(args.Key, args.CommandId, args.ShardId)
			if val == "" {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
			}
			reply.Value = val
			return
		}
	}

	// 向raft log发送op request
	op := Op{
		Key:       args.Key,
		Value:     "",
		Op:        "Get",
		ShardId:   args.ShardId,
		ConfigNum: args.ConfigNum,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	toCommitIdx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
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
		DPrintf("%d of gid %d get in shard %d TIMEOUT", kv.me, kv.gid, args.ShardId)
		reply.Err = ErrTimeOut
	}

	kv.mu.Lock()
	go kv.freeReplyMap(toCommitIdx)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.checkConfigNumMatch(args.ConfigNum) {
		// server的版本号过低，超时退出
		reply.Err = ErrTimeOut
		DPrintf("%d of gid %d config number %d not match request %d in PUTAPPEND, refuse", kv.me, kv.gid, kv.config.Num, args.ConfigNum)
		return
	}

	if atomic.LoadInt32(&kv.sharding) == 1 {
		// 正在分片中，直接超时返回
		reply.Err = ErrTimeOut
		DPrintf("%d of gid %d is sharding, refuse any outer requests", kv.me, kv.gid)
		return
	}

	// 判断该request是否属于group拥有shard
	if !kv.checkShard(args.ShardId) {
		DPrintf("%d of gid %d wrong group in putappend, input shard %d", kv.me, kv.gid, args.ShardId)
		reply.Err = ErrWrongGroup
		return
	}

	// 判断该op是否已经commit到raft log
	if lastOpIdx, ok := kv.lastOpMap[args.ShardId][args.ClientId]; ok {
		if lastOpIdx >= args.CommandId {
			// op索引已过期，直接返回，避免op重复执行
			reply.Err = OK
			DPrintf("%d put append return early in PutAppend, lastOpIdx %d, command id %d", kv.me, lastOpIdx, args.CommandId)
			return
		}
	}

	// 向raft log发送op request
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ShardId:   args.ShardId,
		ConfigNum: args.ConfigNum,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	toCommitIdx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
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
		DPrintf("%d of gid %d putappend in shard %d TIMEOUT", kv.me, kv.gid, args.ShardId)
		reply.Err = ErrTimeOut
	}

	kv.mu.Lock()
	go kv.freeReplyMap(toCommitIdx)
}

func (kv *ShardKV) ShardTransfer(args *ShardTransferArgs, reply *ShardTransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if args.ConfigNum > kv.config.Num {
		// server的版本号过低，超时退出
		reply.Err = ErrTimeOut
		DPrintf("%d of gid %d config number %d smaller than request %d in SHARDTRANSFER, refuse", kv.me, kv.gid, kv.config.Num, args.ConfigNum)
		return
	}

	if atomic.LoadInt32(&kv.sharding) == 1 {
		// 正在分片中，直接超时返回
		reply.Err = ErrTimeOut
		DPrintf("%d of gid %d is sharding, refuse any outer requests", kv.me, kv.gid)
		return
	}

	// 内部通讯，不需要dedup
	// 向raft log发送op request
	op := Op{
		Op:        "ShardTransfer",
		ShardId:   args.ShardId,
		ConfigNum: args.ConfigNum,
	}
	toCommitIdx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 开始transferring
	if args.BEGIN {
		atomic.StoreInt32(&kv.transferring, 1)
		DPrintf("%d of gid %d transferring START...", kv.me, kv.gid)
	}

	// 创建toCommitIdx对应的响应channel
	replyCh := make(chan ReplyMsg)
	kv.replyChMap[toCommitIdx] = replyCh
	kv.mu.Unlock()

	// 等待execMsg后的响应
	select {
	case replyMsg := <-replyCh:
		reply.Err = replyMsg.Err
		if reply.Err == OK {
			// OK的话，copy shard data into reply
			reply.ShardData = make(map[string]map[int]string)
			for key, pair := range replyMsg.ShardData {
				if _, ok := reply.ShardData[key]; !ok {
					reply.ShardData[key] = make(map[int]string)
				}
				for id, val := range pair {
					reply.ShardData[key][id] = val
				}
			}
			// copy shard last op into reply
			reply.ShardLastOp = make(map[int]int)
			for clientId, lastCommandId := range replyMsg.ShardLastOp {
				reply.ShardLastOp[clientId] = lastCommandId
			}
		}
	case <-time.After(500 * time.Millisecond):
		DPrintf("%d of gid %d transfer shard %d to %d TIMEOUT", kv.me, kv.gid, args.ShardId, args.GID)
		reply.Err = ErrTimeOut
	}

	kv.mu.Lock()
	// 结束transferring
	if args.END {
		atomic.StoreInt32(&kv.transferring, 0)
		DPrintf("%d of gid %d transferring END", kv.me, kv.gid)
	}
	go kv.freeReplyMap(toCommitIdx)

}

func (kv *ShardKV) ShardChange() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 仅有leader需要提交config相关op，follower do nothing，直接return
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}

	if atomic.LoadInt32(&kv.transferring) == 1 {
		// 正在传输shard中
		DPrintf("%d of gid %d is transferring, sharding later", kv.me, kv.gid)
		return
	}

	if atomic.LoadInt32(&kv.sharding) == 1 {
		// 正在分片中，直接返回
		DPrintf("%d of gid %d is sharding, refuse any outer requests", kv.me, kv.gid)
		return
	}

	// DPrintf("%d of gid %d querying config number  %d ...", kv.me, kv.gid, kv.config.Num+1)
	// tchannel := make(chan shardctrler.Config)
	// var config shardctrler.Config
	// for {
	// 	go func() {
	// 		config := kv.mck.Query(kv.config.Num + 1)
	// 		tchannel <- config
	// 	}()
	// 	select {
	// 	case config = <-tchannel:
	// 	case <-time.After(5000 * time.Millisecond):
	// 		DPrintf("%d of gid %d query config %d for 5s FAILED, try again...", kv.me, kv.gid, kv.config.Num+1)
	// 		kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	// 	}

	// 	if config.Num > 0 {
	// 		break
	// 	}
	// }
	config := kv.mck.Query(kv.config.Num + 1)
	DPrintf("%d of gid %d current config number %d, received config number %d CHECKOUT", kv.me, kv.gid, kv.config.Num, config.Num)
	if config.Num <= kv.config.Num {
		return
	}

	// 开始sharding
	atomic.StoreInt32(&kv.sharding, 1)
	DPrintf("%d of gid %d config number sequence %d ===> %d sharding START...", kv.me, kv.gid, kv.config.Num, config.Num)
	// 等待分片完成
	succeed := kv.Sharding(config)
	// 结束sharding
	atomic.StoreInt32(&kv.sharding, 0)
	if succeed {
		DPrintf("%d of gid %d config number sequence %d ===> %d sharding SUCCEED", kv.me, kv.gid, config.Num-1, config.Num)
		// 每次shard成功之后，snapshot保存一下，避免重复sharding
		// kv.doKVSnapshot()
	} else {
		DPrintf("%d of gid %d config number sequence %d ===> %d sharding FAILED", kv.me, kv.gid, config.Num-1, config.Num)
	}
}

func (kv *ShardKV) FastPutBatch(dataBatch map[string]map[int]string, shardId int, configNum int) bool {
	// 向raft log发送op request
	op := Op{
		DataBatch: dataBatch,
		Op:        "FastPutBatch",
		ShardId:   shardId,
		ConfigNum: configNum,
	}

	toCommitIdx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}

	// 创建toCommitIdx对应的响应channel
	replyCh := make(chan ReplyMsg)
	kv.replyChMap[toCommitIdx] = replyCh
	kv.mu.Unlock()

	succeed := false
	// 等待execMsg后的响应
	select {
	case replyMsg := <-replyCh:
		if replyMsg.Err == OK {
			succeed = true
		}
	case <-time.After(1000 * time.Millisecond):
		DPrintf("%d of gid %d fastputbatch for shard %d TIMEOUT", kv.me, kv.gid, shardId)
		succeed = false
	}

	kv.mu.Lock()
	go kv.freeReplyMap(toCommitIdx)
	return succeed
}

func (kv *ShardKV) SyncConfig(config shardctrler.Config, LastOpMap map[int]map[int]int) bool {
	// 向raft log发送op request

	op := Op{
		Key:       "",
		Value:     "",
		Op:        "SyncConfig",
		Config:    config,
		ConfigNum: config.Num,
		LastOpMap: LastOpMap,
	}

	toCommitIdx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}

	// 创建toCommitIdx对应的响应channel
	replyCh := make(chan ReplyMsg)
	kv.replyChMap[toCommitIdx] = replyCh
	kv.mu.Unlock()

	succeed := false
	// 等待execMsg后的响应
	select {
	case replyMsg := <-replyCh:
		if replyMsg.Err == OK {
			succeed = true
		}
	case <-time.After(500 * time.Millisecond):
		DPrintf("%d of gid %d syncconfig number %d TIMEOUT", kv.me, kv.gid, config.Num)
		succeed = false
	}

	kv.mu.Lock()
	go kv.freeReplyMap(toCommitIdx)
	return succeed
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) overThreshold() bool {
	// maxraftstate为-1说明不做限制，永远不会到达阈值
	if kv.maxraftstate == -1 {
		return false
	}

	// 90% maxraftstate设定为阈值
	prop := float32(kv.rf.GetRaftStateSize()) / float32(kv.maxraftstate)
	// DPrintf("%d of gid %d prop %f", kv.me, kv.gid, prop)
	return prop >= 0.9
}

func (kv *ShardKV) doKVSnapshot(index int) {
	// kv database
	// lastOpMap
	// 需要做snapshot保存快照
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	e.Encode(kv.database)
	e.Encode(kv.lastOpMap)
	e.Encode(kv.config)

	data := b.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) loadKVSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshot := kv.rf.GetSnapshot()
	DPrintf("%d of gid %d loadk KV snapshot, length %d", kv.me, kv.gid, len(snapshot))

	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	b := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(b)
	d.Decode(&kv.database)
	d.Decode(&kv.lastOpMap)
	d.Decode(&kv.config)
}

func (kv *ShardKV) applyOp(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := msg.Command.(Op)
	commitIdx := msg.CommandIndex
	replyTerm := msg.CommandTerm

	if _, isLeader := kv.rf.GetState(); isLeader {
		DPrintf("%d of gid %d op %s, clientId %d, commandId %d, commitIdx %d, term %d, key %s, value %s, shard %d, config number %d, server config number %d", kv.me, kv.gid, op.Op, op.ClientId, op.CommandId, commitIdx, replyTerm, op.Key, op.Value, op.ShardId, op.ConfigNum, kv.config.Num)
	}

	// 判断shard是否依然有效，仅仅在get和put中进行判断
	if (op.Op == "Get" || op.Op == "Put" || op.Op == "Append") && !kv.checkShard(op.ShardId) {
		// 无效shard，放任超时
		DPrintf("%d of gid %d wrong group in applyOp %s, input shard %d", kv.me, kv.gid, op.Op, op.ShardId)
		return
	}

	// 判断op是否过期，仅仅在get和put中进行判断
	if op.Op == "Get" || op.Op == "Put" || op.Op == "Append" {
		if lastOpIdx, ok := kv.lastOpMap[op.ShardId][op.ClientId]; ok {
			if lastOpIdx >= op.CommandId {
				// 过期，放任超时
				DPrintf("%d of gid %d stale op %s, lastOpIdx %d, client id %d, command id %d", kv.me, kv.gid, op.Op, lastOpIdx, op.ClientId, op.CommandId)
				return
			}
		}
	}

	var replyMsg ReplyMsg
	// op为全新op，执行
	if op.Op == "Get" {
		// 读取Key，不影响database
		val := kv.getByKeyThenUpdate(op.Key, op.CommandId, op.ShardId)
		replyMsg.Err = OK
		replyMsg.Value = val
	} else if op.Op == "Put" {
		// 直接设置key
		kv.setByKeyAndId(op.Key, op.CommandId, op.ShardId, op.Value)
		replyMsg.Err = OK
	} else if op.Op == "Append" {
		// append
		// 先get
		val := kv.getByKeyAndId(op.Key, -1, op.ShardId)
		new_val := val + op.Value
		kv.setByKeyAndId(op.Key, op.CommandId, op.ShardId, new_val)
		replyMsg.Err = OK
	} else if op.Op == "FastPut" {
		kv.setByKeyAndId(op.Key, op.CommandId, op.ShardId, op.Value)
		replyMsg.Err = OK
	} else if op.Op == "FastPutBatch" {

		for key, pair := range op.DataBatch {
			for id, val := range pair {
				kv.setByKeyAndId(key, id, op.ShardId, val)
			}
		}
		replyMsg.Err = OK

	} else if op.Op == "SyncConfig" {
		// copy config op into kv state
		if kv.config.Num < op.ConfigNum {
			kv.config.Num = op.Config.Num
			kv.config.Groups = make(map[int][]string)
			for k, v := range op.Config.Groups {
				kv.config.Groups[k] = make([]string, 0)
				kv.config.Groups[k] = append(kv.config.Groups[k], v...)
			}
			kv.config.Shards = [shardctrler.NShards]int{}
			for i := 0; i < shardctrler.NShards; i++ {
				kv.config.Shards[i] = op.Config.Shards[i]
			}
			// copy last op into kv state
			for shard, subdb := range op.LastOpMap {
				kv.lastOpMap[shard] = make(map[int]int)
				for k, v := range subdb {
					kv.lastOpMap[shard][k] = v
				}
			}
			replyMsg.Err = OK
		} else {
			replyMsg.Err = ErrTimeOut
		}

	} else if op.Op == "ShardTransfer" {
		// copy shard data to replyMsg
		replyMsg.ShardData = make(map[string]map[int]string)
		replyMsg.Err = OK
		if subdb, ok := kv.database[op.ShardId]; ok {
			for key, pair := range subdb {
				if _, ok := replyMsg.ShardData[key]; !ok {
					replyMsg.ShardData[key] = make(map[int]string)
				}
				for id, val := range pair {
					replyMsg.ShardData[key][id] = val
				}
			}
		}
		// copy shard last op into replyMsg
		replyMsg.ShardLastOp = make(map[int]int)
		if subdb, ok := kv.lastOpMap[op.ShardId]; ok {
			for clientId, lastCommandId := range subdb {
				replyMsg.ShardLastOp[clientId] = lastCommandId
			}
		}
	}

	// 更新对应client的最新op id
	// 仅更新get和putappend相关op
	if replyMsg.Err == OK {
		if _, ok := kv.lastOpMap[op.ShardId]; !ok {
			kv.lastOpMap[op.ShardId] = make(map[int]int)
		}
		kv.lastOpMap[op.ShardId][op.ClientId] = op.CommandId
	}

	// 判断term是否过期
	term, isLeader := kv.rf.GetState()
	// 如果exec时的当前term跟op提交时一致，并且kv依然是leader，则成功响应，否则放任超时
	if replyCh, ok := kv.replyChMap[commitIdx]; ok && isLeader && term == replyTerm {
		replyCh <- replyMsg
	}

	// 判断是否需要snapshot
	if kv.overThreshold() {
		kv.doKVSnapshot(commitIdx)
	}
}

func (kv *ShardKV) applySnapshot(msg raft.ApplyMsg) {
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		// raft协议层安装snapshot成功，kv service层再加载其state状态
		DPrintf("%d of gid %d cond install succeed in kv layer, snapshot term %d, snapshot index %d, loadKVSnapshot then", kv.me, kv.gid, msg.SnapshotTerm, msg.CommandIndex)
		kv.loadKVSnapshot()
	}
	// 无需通过channel做任何通知
}

func (kv *ShardKV) execMsg() {
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
				DPrintf("%d of gid %d error applied msg", kv.me, kv.gid)
				kv.Kill()
			}
		case <-time.After(1000 * time.Millisecond):
			// 1000ms内没有消息，修改kv状态
		}
	}
}

func (kv *ShardKV) fetchShardData(targetGid int, args ShardTransferArgs, shardMap map[int]map[string]map[int]string, shardLastOp map[int]map[int]int) bool {
	// 处理单个shard
	reply := ShardTransferReply{
		Err: OK,
	}

	DPrintf("%d of gid %d fetch shard %d for config number %d from gid %d START", kv.me, kv.gid, args.ShardId, kv.config.Num, targetGid)
	if servers, ok := kv.config.Groups[targetGid]; ok {
		// try each server for the shard.
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])

			// 这里需要解锁一下，避免死锁
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.ShardTransfer", &args, &reply)
			kv.mu.Lock()

			if ok && (reply.Err == OK) {
				// shard数据导入
				shardMap[args.ShardId] = reply.ShardData
				shardLastOp[args.ShardId] = reply.ShardLastOp
				DPrintf("%d of gid %d fetch shard %d from gid %d SUCCEED", kv.me, kv.gid, args.ShardId, targetGid)
				return true
			}
			// ... not ok, or ErrWrongLeader
		}
		DPrintf("%d of gid %d fetch shard %d from gid %d TIMEOUT", kv.me, kv.gid, args.ShardId, targetGid)
	}
	return false
}

func (kv *ShardKV) Sharding(config shardctrler.Config) bool {
	// 该函数只有leader会执行
	// 整个过程sync，hold the mu lock

	// 适配新shards到本server
	// 新增shards map，gid->shard slice
	append_shards := make(map[int][]int)
	for sid, gid := range config.Shards {
		ogid := kv.config.Shards[sid]
		if ogid == 0 {
			continue
		}
		if gid == kv.gid && ogid != kv.gid {
			if _, ok := append_shards[ogid]; !ok {
				append_shards[ogid] = make([]int, 0)
			}
			// 添加到slice
			append_shards[ogid] = append(append_shards[ogid], sid)
		}
	}
	DPrintf("%d of gid %d appending shards %v", kv.me, kv.gid, append_shards)

	fetchedShardMap := make(map[int]map[string]map[int]string)
	fetchedShardLastOp := make(map[int]map[int]int)
	// RPC访问其他group leader获取新增shard数据
	// 逐个shard获取，获取过程中保持lock，即block
	for ogid, shards := range append_shards {
		for i, shard := range shards {
			begin := false
			end := false
			if i == 0 {
				begin = true
			}
			if i == len(shards)-1 {
				end = true
			}
			args := ShardTransferArgs{
				ShardId:   shard,
				GID:       kv.gid,
				ConfigNum: kv.config.Num,
				BEGIN:     begin,
				END:       end,
			}
			fetch_succeed := kv.fetchShardData(ogid, args, fetchedShardMap, fetchedShardLastOp)
			if !fetch_succeed {
				// 只要有一个失败，中断
				n := rand.Intn(1000)
				DPrintf("%d of gid %d fetch shard %d FAILED, sleep for %d ms and return", kv.me, kv.gid, shard, n)
				// 等待一个随机0-1000ms的时间后，返回重试
				time.Sleep(time.Duration(n) * time.Millisecond)
				return false
			}
		}
	}

	// 将fetched shard data发布到group log上
	for shard, dataBatch := range fetchedShardMap {
		succeed := kv.FastPutBatch(dataBatch, shard, kv.config.Num)
		if !succeed {
			DPrintf("%d of gid %d Sharding for %d FAILED", kv.me, kv.gid, shard)
		}
	}

	// 更新server的config状态，废弃shard将不再收到支持
	// copy config
	// DPrintf("%d of gid %d sync config, number from %d to %d", kv.me, kv.gid, kv.config.Num, config.Num)
	return kv.SyncConfig(config, fetchedShardLastOp)
}

func (kv *ShardKV) execShard() {
	for !kv.killed() {
		kv.ShardChange()

		// 90ms 轮询一次
		time.Sleep(90 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.database = make(map[int]map[string]map[int]string)
	kv.lastOpMap = make(map[int]map[int]int)
	kv.replyChMap = make(map[int]chan ReplyMsg)
	atomic.StoreInt32(&kv.dead, 0)
	atomic.StoreInt32(&kv.sharding, 0)
	atomic.StoreInt32(&kv.transferring, 0)

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = kv.mck.Query(0)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 加载缓存snapshot，用于reboot后的状态恢复
	DPrintf("%d of gid %d state restore after booting", kv.me, kv.gid)
	kv.loadKVSnapshot()

	// 起一个协程，不断执行从raft log commit的op或snapshot
	go kv.execMsg()

	// 起一个协程，不断从shardctrls获取config变化
	go kv.execShard()

	DPrintf("%d of gid %d launched.", kv.me, kv.gid)
	return kv
}
