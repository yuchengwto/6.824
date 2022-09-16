package shardctrler

import (
	"encoding/json"
	"log"
	"math"
	"sort"
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type ReplyMsg struct {
	Err    Err
	Config Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	lastOpMap  map[int]int
	replyChMap map[int]chan ReplyMsg
	// gid的数组
	GIds         []int
	GIdToShardId map[int][]int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Op        string
	Args      []byte
	ClientId  int
	CommandId int
}

func (sc *ShardCtrler) freeReplyMap(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.replyChMap, index)
}

// func copyConfig(trg *Config, src *Config) {
// 	trg.Num = src.Num
// 	trg.Shards = src.Shards
// 	trg.Groups = make(map[int][]string)
// 	for k, v := range src.Groups {
// 		if _, ok := trg.Groups[k]; !ok {
// 			trg.Groups[k] = make([]string, 0)
// 		}

// 		trg.Groups[k] = append(trg.Groups[k], v...)
// 	}
// }

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if lastOpIdx, ok := sc.lastOpMap[args.ClientId]; ok {
		if lastOpIdx >= args.CommandId {
			reply.Err = ErrDup
			DPrintf("%d Join duplicated, client id: %d, command id: %d", sc.me, args.ClientId, args.CommandId)
			return
		}
	}

	Args, _ := json.Marshal(args)
	op := Op{
		Op:        "Join",
		Args:      Args,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}

	toCommitIdx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.replyChMap[toCommitIdx] = make(chan ReplyMsg)
	replyCh := sc.replyChMap[toCommitIdx]
	sc.mu.Unlock()

	// 等待完成或超时
	select {
	case replyMsg := <-replyCh:
		reply.Err = replyMsg.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	sc.mu.Lock()
	go sc.freeReplyMap(toCommitIdx)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if lastOpIdx, ok := sc.lastOpMap[args.ClientId]; ok {
		if lastOpIdx >= args.CommandId {
			reply.Err = ErrDup
			DPrintf("%d Leave duplicated, client id: %d, command id: %d", sc.me, args.ClientId, args.CommandId)
			return
		}
	}

	Args, _ := json.Marshal(args)
	op := Op{
		Op:        "Leave",
		Args:      Args,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}

	toCommitIdx, _, isLeader := sc.rf.Start(op)
	// DPrintf("%d leave to commitIdx %d, isLeader %t\n", sc.me, toCommitIdx, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.replyChMap[toCommitIdx] = make(chan ReplyMsg)
	replyCh := sc.replyChMap[toCommitIdx]
	sc.mu.Unlock()

	// 等待完成或超时
	select {
	case replyMsg := <-replyCh:
		reply.Err = replyMsg.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	sc.mu.Lock()
	go sc.freeReplyMap(toCommitIdx)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if lastOpIdx, ok := sc.lastOpMap[args.ClientId]; ok {
		if lastOpIdx >= args.CommandId {
			reply.Err = ErrDup
			DPrintf("%d Move duplicated, client id: %d, command id: %d", sc.me, args.ClientId, args.CommandId)
			return
		}
	}

	Args, _ := json.Marshal(args)
	op := Op{
		Op:        "Move",
		Args:      Args,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}

	toCommitIdx, _, isLeader := sc.rf.Start(op)
	// DPrintf("%d move to commitIdx %d, isLeader %t\n", sc.me, toCommitIdx, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.replyChMap[toCommitIdx] = make(chan ReplyMsg)
	replyCh := sc.replyChMap[toCommitIdx]
	sc.mu.Unlock()

	// 等待完成或超时
	select {
	case replyMsg := <-replyCh:
		reply.Err = replyMsg.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	sc.mu.Lock()
	go sc.freeReplyMap(toCommitIdx)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if lastOpIdx, ok := sc.lastOpMap[args.ClientId]; ok {
		if lastOpIdx >= args.CommandId {
			reply.Err = ErrDup
			DPrintf("%d Query duplicated, client id: %d, command id: %d", sc.me, args.ClientId, args.CommandId)
			if args.Num == -1 || args.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			return
		}
	}

	Args, _ := json.Marshal(args)
	op := Op{
		Op:        "Query",
		Args:      Args,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}

	toCommitIdx, _, isLeader := sc.rf.Start(op)
	// DPrintf("%d query to commitIdx %d, isLeader %t\n", sc.me, toCommitIdx, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.replyChMap[toCommitIdx] = make(chan ReplyMsg)
	replyCh := sc.replyChMap[toCommitIdx]
	sc.mu.Unlock()

	// 等待完成或超时
	select {
	case replyMsg := <-replyCh:
		reply.Err = replyMsg.Err
		reply.Config = replyMsg.Config
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	sc.mu.Lock()
	go sc.freeReplyMap(toCommitIdx)
}

// 实际处理Join的回调函数
func (sc *ShardCtrler) handleJoin(args *JoinArgs, reply *ReplyMsg) {
	new_config := new(Config)
	new_config.Groups = make(map[int][]string)

	new_GIds := make([]int, 0)
	// 拷贝原配置中的服务器信息
	for k, v := range sc.configs[len(sc.configs)-1].Groups {
		new_config.Groups[k] = v
	}
	for k, v := range args.Servers {
		// 新增的GIds和服务器信息
		new_config.Groups[k] = v
		new_GIds = append(new_GIds, k)
	}
	// GIds重新排序
	sort.Slice(new_GIds, func(i, j int) bool {
		return new_GIds[i] < new_GIds[j]
	})
	sc.GIds = append(sc.GIds, new_GIds...)
	sort.Slice(sc.GIds, func(i, j int) bool {
		return sc.GIds[i] < sc.GIds[j]
	})
	new_config.Num = len(sc.configs)

	// 均匀分布shard
	// 有新的group加入时，先计算目标均值和余数
	// 余数将在已有groups上依序逐个+1，得到已有group的目标shard数
	// 计算各group当前shard数和目标shard数的差值，插入新group
	// rebalance得到新groups
	mean := NShards / (len(sc.GIds) - 1)
	residual := NShards % (len(sc.GIds) - 1)
	new_residul := 0
	DPrintf("%d before join gid -> shards: %v, join gids: %v", sc.me, sc.GIdToShardId, args.Servers)

	// 初始化的特殊情况，GId 0包含了所有shards
	if len(sc.GIdToShardId[0]) > 0 {
		for _, gid := range new_GIds {
			if residual > 0 {
				sc.GIdToShardId[gid] = make([]int, mean+1)
				copy(sc.GIdToShardId[gid], sc.GIdToShardId[0][len(sc.GIdToShardId[0])-(mean+1):])
				sc.GIdToShardId[0] = sc.GIdToShardId[0][:len(sc.GIdToShardId[0])-(mean+1)]
				residual--
			} else {
				sc.GIdToShardId[gid] = make([]int, mean)
				copy(sc.GIdToShardId[gid], sc.GIdToShardId[0][len(sc.GIdToShardId[0])-(mean):])
				sc.GIdToShardId[0] = sc.GIdToShardId[0][:len(sc.GIdToShardId[0])-(mean)]
			}
		}
	} else {

		// 接收cut后多出的shard，用于后续新增gid的分配
		buffer := make([]int, 0)
		// 遍历已有gid，cut
		for idx, gid := range sc.GIds {
			if idx == 0 {
				// 跳过无效的gid 0
				continue
			}
			if _, ok := args.Servers[gid]; ok {
				// 跳过新增gid
				if residual > 0 {
					residual--
					new_residul++
				}
				continue
			}
			if residual > 0 {
				// 向buffer中追加待cut掉的shards
				buffer = append(buffer, sc.GIdToShardId[gid][min(mean+1, len(sc.GIdToShardId[gid])):]...)
				// cut已有group的shards
				sc.GIdToShardId[gid] = sc.GIdToShardId[gid][:min(mean+1, len(sc.GIdToShardId[gid]))]
				residual--
			} else {
				buffer = append(buffer, sc.GIdToShardId[gid][min(mean, len(sc.GIdToShardId[gid])):]...)
				sc.GIdToShardId[gid] = sc.GIdToShardId[gid][:min(mean, len(sc.GIdToShardId[gid]))]
			}
		}
		sort.Slice(buffer, func(i, j int) bool {
			return buffer[i] < buffer[j]
		})

		// 遍历新gid，从buffer分配
		for _, gid := range new_GIds {
			if new_residul > 0 {
				sc.GIdToShardId[gid] = make([]int, mean+1)
				copy(sc.GIdToShardId[gid], buffer[len(buffer)-(mean+1):])
				buffer = buffer[:len(buffer)-(mean+1)]
				new_residul--
			} else {
				sc.GIdToShardId[gid] = make([]int, mean)
				copy(sc.GIdToShardId[gid], buffer[len(buffer)-(mean):])
				buffer = buffer[:len(buffer)-(mean)]
			}
		}
	}

	// 追加新config
	for gid, sids := range sc.GIdToShardId {
		if len(sids) > 0 {
			for _, sid := range sids {
				new_config.Shards[sid] = gid
			}
		}
	}
	sc.configs = append(sc.configs, *new_config)
	DPrintf("%d after join gid -> shards: %v", sc.me, sc.GIdToShardId)
}

func (sc *ShardCtrler) handleLeave(args *LeaveArgs, reply *ReplyMsg) {

	leave_gids := args.GIDs
	// buffer用来存储leave groups中包含的shards，分配给剩余groups
	buffer := []int{}
	DPrintf("%d before leave gid -> shards: %v, leave gids: %v", sc.me, sc.GIdToShardId, leave_gids)
	for _, gid := range leave_gids {
		buffer = append(buffer, sc.GIdToShardId[gid]...)
		delete(sc.GIdToShardId, gid)
	}
	sort.Slice(buffer, func(i, j int) bool {
		return buffer[i] < buffer[j]
	})

	// 删除leave gids in sc.GIds
	tmp := []int{}
	found := false
	for _, gid := range sc.GIds {
		found = false
		for _, lgid := range leave_gids {
			if gid == lgid {
				found = true
				break
			}
		}
		if !found {
			tmp = append(tmp, gid)
		}
	}
	// 更新gid至最新
	sc.GIds = tmp
	if len(sc.GIds) == 1 {
		sc.GIdToShardId[sc.GIds[0]] = buffer
	} else {
		mean := len(buffer) / (len(sc.GIds) - 1)
		residual := len(buffer) % (len(sc.GIds) - 1)

		// 先找到shards长度最小的gid，从该gid开始
		minidx := 1
		minlen := math.MaxInt
		for idx, gid := range sc.GIds {
			if idx == 0 {
				continue
			}
			if len(sc.GIdToShardId[gid]) < minlen {
				minlen = len(sc.GIdToShardId[gid])
				minidx = idx
			}
		}

		// 从idx开始分配buffer shards
		idx := minidx
		for len(buffer) > 0 {
			gid := sc.GIds[idx]
			// DPrintf("%d gid %d, idx %d, minidx %d, gidsize %d, mean %d, residual %d, buffer: %v, gts: %v", sc.me, gid, idx, minidx, len(sc.GIds), mean, residual, buffer, sc.GIdToShardId)

			// 循环到下一索引
			idx = (idx + 1) % (len(sc.GIds))
			if gid == 0 {
				continue
			}

			if residual > 0 {
				sc.GIdToShardId[gid] = append(sc.GIdToShardId[gid], buffer[len(buffer)-(mean+1):]...)
				buffer = buffer[:len(buffer)-(mean+1)]
				residual--
			} else {
				sc.GIdToShardId[gid] = append(sc.GIdToShardId[gid], buffer[len(buffer)-(mean):]...)
				buffer = buffer[:len(buffer)-(mean)]
			}
		}
	}

	DPrintf("%d after leave gid -> shards: %v", sc.me, sc.GIdToShardId)

	// 追加新config
	new_config := new(Config)
	new_config.Groups = make(map[int][]string)
	new_config.Num = len(sc.configs)
	// 拷贝原配置中的服务器信息
	for k, v := range sc.configs[len(sc.configs)-1].Groups {
		new_config.Groups[k] = v
	}
	// 删除leave gids和对应的服务器信息
	for _, lgid := range leave_gids {
		delete(new_config.Groups, lgid)
	}
	for gid, sids := range sc.GIdToShardId {
		if len(sids) > 0 {
			for _, sid := range sids {
				new_config.Shards[sid] = gid
			}
		}
	}
	sc.configs = append(sc.configs, *new_config)
}

func (sc *ShardCtrler) handleQuery(args *QueryArgs, reply *ReplyMsg) {
	n := args.Num
	if n == -1 || n >= len(sc.configs) {
		// 拷贝返回最新config
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[n]
	}
}

func (sc *ShardCtrler) handleMove(args *MoveArgs, reply *ReplyMsg) {
	// 待move shard的当前gid
	cgid := sc.configs[len(sc.configs)-1].Shards[args.Shard]
	// 移除cgid中的对应shard
	for idx, sid := range sc.GIdToShardId[cgid] {
		if sid == args.Shard {
			sc.GIdToShardId[cgid] = append(sc.GIdToShardId[cgid][:idx], sc.GIdToShardId[cgid][idx+1:]...)
			break
		}
	}
	// 添加shard到目标gid
	sc.GIdToShardId[args.GID] = append(sc.GIdToShardId[args.GID], args.Shard)

	// 追加新的config
	new_config := new(Config)
	new_config.Groups = make(map[int][]string)
	new_config.Num = len(sc.configs)
	// 拷贝原配置中的服务器信息
	for k, v := range sc.configs[len(sc.configs)-1].Groups {
		new_config.Groups[k] = v
	}
	for gid, sids := range sc.GIdToShardId {
		if len(sids) > 0 {
			for _, sid := range sids {
				new_config.Shards[sid] = gid
			}
		}
	}
	sc.configs = append(sc.configs, *new_config)
}

func (sc *ShardCtrler) execMsg() {
	// 逐个应用raft已commit的op
	for !sc.killed() {
		msg := <-sc.applyCh
		// 从applyCh中获取已提交的msg
		if msg.CommandValid {
			sc.applyOp(msg)
		} else {
			DPrintf("%d error applied msg", sc.me)
			sc.Kill()
		}
	}
}

func (sc *ShardCtrler) applyOp(msg raft.ApplyMsg) {

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 判断term是否过期
	term, isLeader := sc.rf.GetState()

	op := msg.Command.(Op)
	commitIdx := msg.CommandIndex
	replyTerm := msg.CommandTerm

	if isLeader {
		DPrintf("%d op %s, clientId %d, commandId %d, commitIdx %d, term %d", sc.me, op.Op, op.ClientId, op.CommandId, commitIdx, replyTerm)
	}

	if lastOpIdx, ok := sc.lastOpMap[op.ClientId]; ok {
		if lastOpIdx >= op.CommandId {
			// 放任超时
			DPrintf("%d duplicated in applyOp.", sc.me)
			return
		}
	}

	var replyMsg ReplyMsg
	replyMsg.Err = OK
	if op.Op == "Join" {
		var args JoinArgs
		json.Unmarshal([]byte(op.Args), &args)
		sc.handleJoin(&args, &replyMsg)
	} else if op.Op == "Leave" {
		var args LeaveArgs
		json.Unmarshal([]byte(op.Args), &args)
		sc.handleLeave(&args, &replyMsg)
	} else if op.Op == "Query" {
		var args QueryArgs
		json.Unmarshal([]byte(op.Args), &args)
		sc.handleQuery(&args, &replyMsg)
	} else if op.Op == "Move" {
		var args MoveArgs
		json.Unmarshal([]byte(op.Args), &args)
		sc.handleMove(&args, &replyMsg)
	}

	// 更新对应client的最新op id
	sc.lastOpMap[op.ClientId] = op.CommandId

	// 如果exec时的当前term跟op提交时一致，并且sc依然是leader，则成功响应，否则放任超时
	if isLeader && term == replyTerm {
		if replyCh, ok := sc.replyChMap[commitIdx]; ok {
			select {
			case replyCh <- replyMsg:
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	// Config的Shards会初始化为0的数组

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastOpMap = make(map[int]int)
	sc.replyChMap = make(map[int]chan ReplyMsg)
	// 初始化gid 0对应全部shardId
	sc.GIdToShardId = make(map[int][]int)
	sc.GIdToShardId[0] = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	sc.GIds = append(sc.GIds, 0)

	go sc.execMsg()
	DPrintf("%d launched...", sc.me)
	return sc
}
