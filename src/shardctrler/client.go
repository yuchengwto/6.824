package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	leaderId  int
	commandId int
	me        int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderId = 0
	ck.commandId = 0
	ck.me = int(nrand())
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.CommandId = ck.commandId
	args.ClientId = ck.me

	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)

		if ok && reply.Err == ErrDup {
			// 重复操作，直接返回
			return reply.Config
		}

		if ok && reply.Err == OK {
			ck.commandId++
			return reply.Config
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.CommandId = ck.commandId
	args.ClientId = ck.me

	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.Err == ErrDup {
			// 重复操作，直接返回
			return
		}

		if ok && reply.Err == OK {
			// 执行成功，commandid+1
			ck.commandId += 1
			return
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.CommandId = ck.commandId
	args.ClientId = ck.me

	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.Err == ErrDup {
			// 重复操作，直接返回
			return
		}

		if ok && reply.Err == OK {
			ck.commandId += 1
			return
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.CommandId = ck.commandId
	args.ClientId = ck.me

	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.Err == ErrDup {
			// 重复操作，直接返回
			return
		}

		if ok && reply.Err == OK {
			ck.commandId += 1
			return
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
