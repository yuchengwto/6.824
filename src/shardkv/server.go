package shardkv

import (
	"bytes"
	"log"
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
	Op        string
	ShardId   int
	Config    shardctrler.Config
	ClientId  int
	CommandId int
}

type ShardTransferArgs struct {
	ShardId int
	GID     int
}

type ShardTransferReply struct {
	Err       Err
	ShardData map[string]map[int]string
}

type ReplyMsg struct {
	Err       Err
	Value     string
	ShardData map[string]map[int]string
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
	database         map[int]map[string]map[int]string
	lastOpMap        map[int]int
	replyChMap       map[int]chan ReplyMsg
	dead             int32
	mck              *shardctrler.Clerk
	config           shardctrler.Config
	transferringGIDs map[int]int
}

func (kv *ShardKV) checkShard(shardId int) bool {
	return kv.config.Shards[shardId] == kv.gid
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
			}
		}
	}

	DPrintf("%d of gid %d key %s, id %d, shardId %d not exist", kv.me, kv.gid, key, id, shardId)
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
		pair[-1] = val
	} else {
		// key不存在
		kv.database[shardId][key] = make(map[int]string)
		kv.database[shardId][key][id] = val
		kv.database[shardId][key][-1] = val
	}

	// DPrintf("%d new set key %s, value %s, latest value %s", kv.me, key, kv.database[shardId][key][id], kv.database[shardId][key][-1])
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 判断该request是否属于group拥有shard
	if !kv.checkShard(args.ShardId) {
		DPrintf("%d of gid %d wrong group in get, input shard %d", kv.me, kv.gid, args.ShardId)
		reply.Err = ErrWrongGroup
		reply.Value = ""
		return
	}

	// 判断该op是否已经commit到raft log
	if lastOpIdx, ok := kv.lastOpMap[args.ClientId]; ok {
		if lastOpIdx >= args.CommandId {
			// op索引已过期，直接取值返回
			DPrintf("%d get return early in Get, lastOpIdx %d, command id %d", kv.me, lastOpIdx, args.CommandId)
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

	// 判断该request是否属于group拥有shard
	if !kv.checkShard(args.ShardId) {
		DPrintf("%d of gid %d wrong group in putappend, input shard %d", kv.me, kv.gid, args.ShardId)
		reply.Err = ErrWrongGroup
		return
	}

	// 判断该op是否已经commit到raft log
	if lastOpIdx, ok := kv.lastOpMap[args.ClientId]; ok {
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

// 适配config变化的函数，用以提交变化op，以及等待变化完成
func (kv *ShardKV) ShardChange() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 内部通讯，不需要dedup

	// 仅有leader需要提交config相关op，follower do nothing，直接return
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}

	config := kv.mck.Query(kv.config.Num + 1)
	DPrintf("%d of gid %d check config, old config number %d to new config number %d", kv.me, kv.gid, kv.config.Num, config.Num)
	if config.Num <= kv.config.Num {
		// config number无变化，直接返回
		return
	}

	// 发送op到raft层
	// group internal op
	op := Op{
		Op:     "ShardChange",
		Config: config,
	}

	toCommitIdx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	// 创建toCommitIdx对应的响应channel
	replyCh := make(chan ReplyMsg)
	kv.replyChMap[toCommitIdx] = replyCh
	kv.mu.Unlock()

	// 等待execMsg后的响应
	select {
	case replyMsg := <-replyCh:
		if replyMsg.Err == OK {
			DPrintf("%d of gid %d ShardChange SUCCEED", kv.me, kv.gid)
		} else {
			DPrintf("%d of gid %d ShardChange FAILED", kv.me, kv.gid)
		}
	case <-time.After(500 * time.Millisecond):
		DPrintf("%d of gid %d ShardChange TIMEOUT", kv.me, kv.gid)
	}

	kv.mu.Lock()
	go kv.freeReplyMap(toCommitIdx)
}

func (kv *ShardKV) ShardTransfer(args *ShardTransferArgs, reply *ShardTransferReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 内部通讯，不需要dedup

	// 向raft log发送op request
	op := Op{
		Key:     "",
		Value:   "",
		Op:      "ShardTransfer",
		ShardId: args.ShardId,
	}
	toCommitIdx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 将该gid加入transferringGIDs，避免双向通讯
	kv.transferringGIDs[args.GID] = 1

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
		}
	case <-time.After(500 * time.Millisecond):
		DPrintf("%d of gid %d transfer shard %d to %d TIMEOUT", kv.me, kv.gid, args.ShardId, args.GID)
		reply.Err = ErrTimeOut
	}

	kv.mu.Lock()
	// 结束后，将GID移出
	delete(kv.transferringGIDs, args.GID)
	go kv.freeReplyMap(toCommitIdx)

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
	DPrintf("%d of gid %d prop %f", kv.me, kv.gid, prop)
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

	// e.Encode(kv.commandId)
	e.Encode(kv.config)

	data := b.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) loadKVSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshot := kv.rf.GetSnapshot()
	DPrintf("%d of gid %d install snapshot, length %d", kv.me, kv.gid, len(snapshot))

	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	b := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(b)
	var database map[int]map[string]map[int]string
	var lastOpMap map[int]int
	d.Decode(&database)
	kv.database = database
	d.Decode(&lastOpMap)
	kv.lastOpMap = lastOpMap

	// var commandId int
	// d.Decode(&commandId)
	// kv.commandId = commandId

	var config shardctrler.Config
	d.Decode(&config)
	kv.config = config
}

func (kv *ShardKV) applyOp(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := msg.Command.(Op)
	commitIdx := msg.CommandIndex
	replyTerm := msg.CommandTerm

	DPrintf("%d of gid %d op %s, clientId %d, commandId %d, commitIdx %d, term %d, key %s, value %s, shard %d", kv.me, kv.gid, op.Op, op.ClientId, op.CommandId, commitIdx, replyTerm, op.Key, op.Value, op.ShardId)

	// 判断shard是否依然有效，仅仅在get和put中进行判断
	if op.Op != "ShardChange" && op.Op != "ShardTransfer" && !kv.checkShard(op.ShardId) {
		// 无效shard，放任超时
		DPrintf("%d of gid %d wrong group in applyOp %s, input shard %d", kv.me, kv.gid, op.Op, op.ShardId)
		return
	}

	// 判断op是否过期，仅仅在get和put中进行判断
	if op.Op != "ShardChange" && op.Op != "ShardTransfer" {
		if lastOpIdx, ok := kv.lastOpMap[op.ClientId]; ok {
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
		val := kv.getByKeyAndId(op.Key, -1, op.ShardId)
		replyMsg.Err = OK
		replyMsg.Value = val
	} else if op.Op == "Put" {
		// 直接设置key
		kv.setByKeyAndId(op.Key, op.CommandId, op.ShardId, op.Value)
		replyMsg.Err = OK
		replyMsg.Value = ""
	} else if op.Op == "Append" {
		// append
		// 先get
		val := kv.getByKeyAndId(op.Key, -1, op.ShardId)
		new_val := val + op.Value
		kv.setByKeyAndId(op.Key, op.CommandId, op.ShardId, new_val)
		replyMsg.Err = OK
		replyMsg.Value = ""
	} else if op.Op == "ShardChange" {
		kv.handleShardChange(op.Config)
		replyMsg.Err = OK
		// // ShardChange没有channel等待，函数完成后可以直接返回
		// return
	} else if op.Op == "ShardTransfer" {
		// copy shard data to replyMsg
		replyMsg.ShardData = make(map[string]map[int]string)
		if subdatabase, ok := kv.database[op.ShardId]; !ok {
			// shard不存在
			// 依然返回OK
			replyMsg.Err = OK
		} else {
			replyMsg.Err = OK
			for key, pair := range subdatabase {
				if _, ok := replyMsg.ShardData[key]; !ok {
					replyMsg.ShardData[key] = make(map[int]string)
				}
				for id, val := range pair {
					replyMsg.ShardData[key][id] = val
				}
			}
		}

	}

	// 判断term是否过期
	term, isLeader := kv.rf.GetState()
	// 如果exec时的当前term跟op提交时一致，并且kv依然是leader，则成功响应，否则放任超时
	if replyCh, ok := kv.replyChMap[commitIdx]; ok && isLeader && term == replyTerm {
		replyCh <- replyMsg
	}
	// 更新对应client的最新op id
	// 仅更新get和putappend相关op
	if op.Op != "ShardChange" && op.Op != "ShardTransfer" && replyMsg.Err == OK {
		kv.lastOpMap[op.ClientId] = op.CommandId
	}
	// 判断是否需要snapshot
	if kv.overThreshold() {
		kv.doKVSnapshot(commitIdx)
	}
}

func (kv *ShardKV) applySnapshot(msg raft.ApplyMsg) {
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		// raft协议层安装snapshot成功，kv service层再加载其state状态
		DPrintf("%d of gid %d cond install succeed in kv layer, loadKVSnapshot then", kv.me, kv.gid)
		kv.loadKVSnapshot()
	}
	// 无需通过channel做任何通知
}

func (kv *ShardKV) execMsg() {
	// 逐个应用raft已commit的op
	for !kv.killed() {
		msg := <-kv.applyCh
		// 从applyCh中获取已提交的msg
		if msg.CommandValid {
			kv.applyOp(msg)
		} else if msg.SnapshotValid {
			kv.applySnapshot(msg)
		} else {
			DPrintf("%d of gid %d error applied msg", kv.me, kv.gid)
			kv.Kill()
		}
	}
}

func (kv *ShardKV) fetchShardData(shardId int) bool {
	// 处理单个shard
	// 获取待获取shard的原始地址gid
	gid := kv.config.Shards[shardId]

	// 如果对方gid已在transferring当中，直接返回false，等待下次fetch
	if _, ok := kv.transferringGIDs[gid]; ok {
		return false
	}

	args := ShardTransferArgs{
		ShardId: shardId,
		GID:     kv.gid,
	}
	reply := ShardTransferReply{
		Err: OK,
	}

	DPrintf("%d of gid %d fetch shard %d from gid %d START", kv.me, kv.gid, shardId, gid)
	if servers, ok := kv.config.Groups[gid]; ok {
		// try each server for the shard.
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			// 这里需要解锁一下，避免死锁
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.ShardTransfer", &args, &reply)
			kv.mu.Lock()

			if ok && (reply.Err == OK) {
				// shard数据导入
				// 判断shard是否存在
				if _, ok := kv.database[shardId]; !ok {
					kv.database[shardId] = make(map[string]map[int]string)
				}
				// 逐key copy shard data
				for key, pair := range reply.ShardData {
					if _, ok := kv.database[shardId][key]; !ok {
						kv.database[shardId][key] = make(map[int]string)
					}
					for id, val := range pair {
						kv.database[shardId][key][id] = val
					}
				}
				DPrintf("%d of gid %d fetch shard %d from gid %d SUCCEED", kv.me, kv.gid, shardId, gid)
				return true
			}
			// ... not ok, or ErrWrongLeader
		}
		DPrintf("%d of gid %d fetch shard %d from gid %d TIMEOUT", kv.me, kv.gid, shardId, gid)
	}
	return false
}

func (kv *ShardKV) handleShardChange(config shardctrler.Config) {
	// 整个过程sync，hold the mu lock

	// 判断config number，若new_config编号没有更新，则直接返回
	if config.Num <= kv.config.Num {
		DPrintf("%d of gid %d new config number %d not bigger than old %d", kv.me, kv.gid, config.Num, kv.config.Num)
		return
	}

	// 适配新shards到本server
	// 新增shards
	append_shards := make([]int, 0)
	for sid, gid := range config.Shards {
		ogid := kv.config.Shards[sid]
		if ogid == 0 {
			continue
		}
		if gid == kv.gid && ogid != kv.gid {
			// 添加到slice
			append_shards = append(append_shards, sid)
		}
	}
	DPrintf("%d of gid %d old shards %v -> new shards %v, append shards %v", kv.me, kv.gid, kv.config.Shards, config.Shards, append_shards)

	fetch_succeed := true
	// RPC访问其他group leader获取新增shard数据
	// 逐个shard获取，获取过程中保持lock，即block
	for _, shardId := range append_shards {
		fetch_succeed = kv.fetchShardData(shardId)

		if !fetch_succeed {
			// 只要有一个失败，中断
			DPrintf("%d of gid %d fetch shard %d FAILED", kv.me, kv.gid, shardId)
			break
		}
	}

	// 更新server的config状态，废弃shard将不再收到支持
	// copy config
	if fetch_succeed {
		DPrintf("%d of gid %d update config num from %d to %d", kv.me, kv.gid, kv.config.Num, config.Num)
		kv.config.Num = config.Num
		kv.config.Groups = make(map[int][]string)
		for k, v := range config.Groups {
			kv.config.Groups[k] = make([]string, 0)
			kv.config.Groups[k] = append(kv.config.Groups[k], v...)
		}
		kv.config.Shards = [shardctrler.NShards]int{}
		for i := 0; i < shardctrler.NShards; i++ {
			kv.config.Shards[i] = config.Shards[i]
		}
	} else {
		DPrintf("%d of gid %d update config num from %d to %d FAILED", kv.me, kv.gid, kv.config.Num, config.Num)
	}

}

func (kv *ShardKV) checkConfig() {
	for !kv.killed() {
		kv.ShardChange()

		// 100ms 轮询一次
		time.Sleep(100 * time.Millisecond)
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
	kv.lastOpMap = make(map[int]int)
	kv.replyChMap = make(map[int]chan ReplyMsg)
	kv.transferringGIDs = make(map[int]int)

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = kv.mck.Query(0)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 加载缓存snapshot，用于reboot后的状态恢复
	DPrintf("%d of gid %d state restore after booting", kv.me, kv.gid)
	kv.loadKVSnapshot()

	// 起一个协程，不断从shardctrls获取config变化
	go kv.checkConfig()

	// 起一个协程，不断执行从raft log commit的op或snapshot
	go kv.execMsg()
	DPrintf("%d of gid %d launched.", kv.me, kv.gid)

	return kv
}
