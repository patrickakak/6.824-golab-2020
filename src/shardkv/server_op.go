package shardkv

import (
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string // "Put" or "Append" Get,
	ClientId  int64
	MsgId     int64
	ReqId     int64
	ConfigNum int
}

type NotifyMsg struct {
	Err   Err
	Value string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{}
	op.MsgId = args.MsgId
	op.ReqId = nrand()
	op.Key = args.Key
	op.Op = "Get"
	op.ClientId = args.ClientId
	op.ConfigNum = args.ConfigNum
	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{}
	op.MsgId = args.MsgId
	op.ReqId = nrand()
	op.Key = args.Key
	op.Value = args.Value
	op.Op = args.Op
	op.ClientId = args.ClientId
	op.ConfigNum = args.ConfigNum
	reply.Err = kv.waitCmd(op).Err
}

func (kv *ShardKV) removeCh(id int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notifyCh, id)
}

func (kv *ShardKV) waitCmd(op Op) (res NotifyMsg) {
	ch := make(chan NotifyMsg, 1)

	kv.mu.Lock()
	if op.ConfigNum == 0 || op.ConfigNum < kv.config.Num {
		res.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	kv.notifyCh[op.ReqId] = ch
	kv.mu.Unlock()

	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()
	select {
	case res = <-ch:
		kv.removeCh(op.ReqId)
		return
	case <-t.C:
		kv.removeCh(op.ReqId)
		res.Err = ErrTimeOut
		return
	}
}
