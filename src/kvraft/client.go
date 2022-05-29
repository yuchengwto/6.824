package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

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
	// You'll have to add code here.
	ck.leaderId = 0
	ck.commandId = 0
	ck.me = int(nrand())

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("%d get key: %v\n", ck.me, key)

	// You will have to modify this function.

	// 对server端进行远程调用
	// 如果不ok，则随机换一个server再次尝试，直到ok
	args := GetArgs{
		Key:       key,
		ClientId:  ck.me,
		CommandId: ck.commandId,
	}
	reply := GetReply{
		Err:   OK,
		Value: "",
	}
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader && reply.Err != ErrTimeOut {
			// 发送成功，递增commandId
			ck.commandId++
			break
		}
		// 尝试下一个服务器
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}

	// DPrintf("%d get key return: %v => %v\n", ck.me, key, reply.Value)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("%d put key: %v, value: %v, op: %v\n", ck.me, key, value, op)

	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.me,
		CommandId: ck.commandId,
	}
	reply := PutAppendReply{
		Err: OK,
	}

	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader && reply.Err != ErrTimeOut {
			// 发送成功，递增commandId
			ck.commandId++
			break
		}
		// 尝试下一个服务器
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
