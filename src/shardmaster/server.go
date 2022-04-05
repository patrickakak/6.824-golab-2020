package shardmaster

import (
	"sort"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const WaitCmdTimeOut = time.Millisecond * 500

type NotifyMsg struct {
	Err         Err
	WrongLeader bool
	Config      Config
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs     []Config        // indexed by config num
	lastApplies map[int64]msgId // last apply put/append msg
	msgNotify   map[int64]chan NotifyMsg
	stopCh      chan struct{}
}

type Op struct {
	// Your data here.
	MsgId    msgId
	ReqId    int64
	Args     interface{}
	Method   string
	ClientId int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	res := sm.runCmd("Join", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	res := sm.runCmd("Leave", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	res := sm.runCmd("Move", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.Lock()
	if args.Num > 0 && args.Num < len(sm.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sm.getConfigByIndex(args.Num)
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	res := sm.runCmd("Query", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader, reply.Config = res.Err, res.WrongLeader, res.Config
}

func (sm *ShardMaster) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1].Copy()
	} else {
		return sm.configs[idx].Copy()
	}
}

func (sm *ShardMaster) runCmd(method string, mid msgId, clientId int64, args interface{}) NotifyMsg {
	op := Op{}
	op.MsgId = mid
	op.ReqId = nrand()
	op.Args = args
	op.Method = method
	op.ClientId = clientId
	return sm.waitCmd(op)
}

func (sm *ShardMaster) adjustConfig(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		for k := range config.Groups {
			for i := range config.Shards {
				config.Shards[i] = k
			}
		}
	} else if len(config.Groups) <= NShards {
		// every group gets avg shards, remainder shards goes to last group
		avg := NShards / len(config.Groups)
		remainderShardsCnt := NShards - avg*len(config.Groups)
		needLoop := false
		lastGid := 0

	LOOP:
		var gids []int
		for gid := range config.Groups {
			gids = append(gids, gid)
		}
		sort.Ints(gids)
		for _, gid := range gids {
			lastGid = gid
			count := 0
			for _, val := range config.Shards {
				if val == gid {
					count += 1
				}
			}
			if count == avg {
				continue
			} else if count > avg {
				if remainderShardsCnt == 0 {
					cnt := 0
					for i, val := range config.Shards {
						if val == gid {
							if cnt == avg {
								config.Shards[i] = 0
							} else {
								cnt += 1
							}
						}
					}
				} else if remainderShardsCnt > 0 {
					cnt := 0
					for i, val := range config.Shards {
						if val == gid {
							if cnt == avg+remainderShardsCnt {
								config.Shards[i] = 0
							} else {
								if cnt == avg {
									remainderShardsCnt -= 1
								} else {
									cnt += 1
								}
							}
						}
					}
				}
			} else {
				for i, val := range config.Shards {
					if count == avg {
						break
					}
					if val == 0 && count < avg {
						config.Shards[i] = gid
					}
				}
				if count < avg {
					needLoop = true
				}
			}
		}
		if needLoop {
			needLoop = false
			goto LOOP
		}
		if lastGid != 0 {
			for i, val := range config.Shards {
				if val == 0 {
					config.Shards[i] = lastGid
				}
			}
		}
	} else {
		// every gid gets at most one, some group may be idle
		gids := make(map[int]int)
		emptyShards := make([]int, 0, NShards)
		for i, gid := range config.Shards {
			if gid == 0 {
				emptyShards = append(emptyShards, i)
				continue
			}
			if _, ok := gids[gid]; ok {
				emptyShards = append(emptyShards, i)
				config.Shards[i] = 0
			} else {
				gids[gid] = 1
			}
		}
		n := 0
		if len(emptyShards) > 0 {
			var keys []int
			for k := range config.Groups {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, gid := range keys {
				if _, ok := gids[gid]; !ok {
					config.Shards[emptyShards[n]] = gid
					n += 1
				}
				if n >= len(emptyShards) {
					break
				}
			}
		}
	}
}

func (sm *ShardMaster) join(args JoinArgs) {
	config := sm.getConfigByIndex(-1)
	config.Num += 1
	for k, v := range args.Servers {
		config.Groups[k] = v
	}
	sm.adjustConfig(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) leave(args LeaveArgs) {
	config := sm.getConfigByIndex(-1)
	config.Num += 1
	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
		for i, v := range config.Shards {
			if v == gid {
				config.Shards[i] = 0
			}
		}
	}
	sm.adjustConfig(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) move(args MoveArgs) {
	config := sm.getConfigByIndex(-1)
	config.Num += 1
	config.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) waitCmd(op Op) (res NotifyMsg) {
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		res.WrongLeader = true
		return
	}
	sm.mu.Lock()
	ch := make(chan NotifyMsg, 1)
	sm.msgNotify[op.ReqId] = ch
	sm.mu.Unlock()

	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()
	select {
	case res = <-ch:
		sm.removeCh(op.ReqId)
		return
	case <-t.C:
		sm.removeCh(op.ReqId)
		res.WrongLeader = true
		res.Err = ErrTimeout
		return
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.stopCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) removeCh(id int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.msgNotify, id)
}

func (sm *ShardMaster) apply() {
	for {
		select {
		case <-sm.stopCh:
			return
		case msg := <-sm.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)

				sm.mu.Lock()
				isRepeated := sm.isRepeated(op.ClientId, op.MsgId)
				if !isRepeated {
					switch op.Method {
					case "Join":
						sm.join(op.Args.(JoinArgs))
					case "Leave":
						sm.leave(op.Args.(LeaveArgs))
					case "Move":
						sm.move(op.Args.(MoveArgs))
					case "Query":
					default:
						panic("unknown method")
					}
				}
				res := NotifyMsg{}
				res.Err = OK
				res.WrongLeader = false
				if op.Method != "Query" {
					sm.lastApplies[op.ClientId] = op.MsgId
				} else {
					res.Config = sm.getConfigByIndex(op.Args.(QueryArgs).Num)
				}
				if ch, ok := sm.msgNotify[op.ReqId]; ok {
					ch <- res
				}
				sm.mu.Unlock()
			}
		}
	}
}

func (sm *ShardMaster) isRepeated(clientId int64, id msgId) bool {
	if val, ok := sm.lastApplies[clientId]; ok {
		return val == id
	}
	return false
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg, 100)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.lastApplies = make(map[int64]msgId)
	sm.msgNotify = make(map[int64]chan NotifyMsg)
	sm.stopCh = make(chan struct{})

	go sm.apply()
	return sm
}
