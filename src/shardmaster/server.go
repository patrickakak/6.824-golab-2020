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
	stopCh  chan struct{}

	// Your data here.
	msgNotify   map[int64]chan NotifyMsg
	lastApplies map[int64]msgId // last apply put/append msg
	configs     []Config        // indexed by config num
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
	// Your code here.
	res := sm.runCmd("Leave", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	res := sm.runCmd("Move", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) adjustConfig(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		// set shards one gid
		for k := range config.Groups {
			for i := range config.Shards {
				config.Shards[i] = k
			}
		}
	} else if len(config.Groups) <= NShards {
		avg := NShards / len(config.Groups)
		// every gid gets avg shards
		otherShardsCount := NShards - avg*len(config.Groups)
		needLoop := false
		lastGid := 0

	LOOP:
		var keys []int
		for k := range config.Groups {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, gid := range keys {
			lastGid = gid
			count := 0
			// count existed first
			for _, val := range config.Shards {
				if val == gid {
					count += 1
				}
			}

			// whether need to change
			if count == avg {
				continue
			} else if count > avg && otherShardsCount == 0 {
				// cut down to avg
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg {
							config.Shards[i] = 0
						} else {
							c += 1
						}
					}
				}

			} else if count > avg && otherShardsCount > 0 {
				// cut down until othersShardsCount is 0
				// if count > avg, set to 0
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg+otherShardsCount {
							config.Shards[i] = 0
						} else {
							if c == avg {
								otherShardsCount -= 1
							} else {
								c += 1
							}
						}
					}
				}
			} else {
				// count < avg, might have no position
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
		// len(config.Groups) > NShards
		// every gid gets at most one, spare gid available
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
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.Config
}

func (sm *ShardMaster) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1].Copy()
	} else {
		return sm.configs[idx].Copy()
	}
}

func (sm *ShardMaster) runCmd(method string, id msgId, clientId int64, args interface{}) (res NotifyMsg) {
	op := Op{
		MsgId:    id,
		ReqId:    nrand(),
		Args:     args,
		Method:   method,
		ClientId: clientId,
	}
	res = sm.waitCmd(op)
	return
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
	close(sm.stopCh)
	// Your code here, if desired.
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
			if !msg.CommandValid {
				continue
			}
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
			res := NotifyMsg{
				Err:         OK,
				WrongLeader: false,
			}
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
	labgob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.applyCh = make(chan raft.ApplyMsg, 100)
	sm.stopCh = make(chan struct{})
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.lastApplies = make(map[int64]msgId)
	sm.msgNotify = make(map[int64]chan NotifyMsg)

	// Your code here.
	go sm.apply()
	return sm
}
