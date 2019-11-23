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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

//
// log entry structure
//
type LogEntry struct {
	Index 	int
	Term	int
	Command	interface{}		//任意类型？
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.

	state		int		// FOLLOWER, LEADER or CANDIDATE
	// Persistent state on all servers:
	currentTerm	int		// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor	int		// candidateId that received vote in current term (or null if none)
	//votesCount	int		// number of votes
	log			[]LogEntry	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex	int		// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied	int		// index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	nextIndex	[]int	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex	[]int	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// channel
	applyChan	chan ApplyMsg
	exitChan	chan bool
	voteChan	chan bool
	appendLogChan	chan bool
	leaderChan	chan bool	// 传送消息：赢得选举
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.mu.Unlock()
}




//
// example RequestVote RPC arguments structure.选举RPC
//
type RequestVoteArgs struct {
	Term			int		// candidate’s term
	CandidateId 	int		// candidate requesting vote
	LastLogIndex	int		// index of candidate’s last log entry
	LastLogTerm		int		// term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.选举回复
//
type RequestVoteReply struct {
	Term			int		// currentTerm, for candidate to update itself
	VoteGranted		bool	// true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//fmt.Printf("%d receives vote request from %d\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm{	// 收到旧的term， 拒绝投票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		if rf.state != FOLLOWER{
			rf.state = FOLLOWER
			//fmt.Printf("%d becomes follower5\n", rf.me)
		}
		rf.votedFor = -1
		rf.persist()
	}
	reply.Term = rf.currentTerm

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	term := rf.getLastLogTerm()
	index := rf.getLastLogIndex()
	upToDate := false

	if term < args.LastLogTerm || (term == args.LastLogTerm && index <= args.LastLogIndex){
		upToDate = true
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate{
		//fmt.Printf("%d votes for %d \n", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.voteChan <- true

	} else {
		reply.VoteGranted = false
	}
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//向第server个服务器发送requestVote请求，参数为args，回复写在reply里
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) launchElection(){
	if rf.state != CANDIDATE {
		return
	}
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	requestVoteMsg := RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastLogIndex(), rf.getLastLogTerm()}
	rf.mu.Unlock()
	votesCount := 1
	for i := range rf.peers{
		if i == rf.me || rf.state != CANDIDATE {
			continue
		}
		go func(i int){
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, requestVoteMsg, reply)
			if(ok){	//成功发送
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != CANDIDATE || rf.currentTerm != requestVoteMsg.Term{	// 状态有变，返回
					return
				}
				if rf.currentTerm < reply.Term {	// 收到更新的term 变回follower
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					//fmt.Printf("%d becomes follower4\n", rf.me)
					rf.votedFor = -1
					rf.persist()
					return
				}
				if reply.VoteGranted {
					votesCount ++
				}
				if votesCount > len(rf.peers) / 2{
					rf.leaderChan <- true
				}
			}
		}(i)
	}
}

// 心跳RPC
type AppendEntriesArgs struct{
	Term 			int		// leader’s term任期
	LeaderId 		int		// so follower can redirect clients用于将客户端请求重定向到leader
	PrevLogIndex	int 	// index of log entry immediately preceding new ones s
	PrevLogTerm 	int		// term of prevLogIndex entry
	Entries			[]LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit 	int		// leader’s commitIndex
}

// 心跳RPC回复
type AppendEntriesReply struct{
	Term 			int		// currentTerm, for leader to update itself
	Success 		bool	//true if follower contained entry matching prevLogIndex and prevLogTerm
	//ConflictIndex 	int
	//ConflictTerm	int
	NextIndex		int
}

// AppendEntries Handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm{	// 收到旧的term，返回false
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm{	//收到更新的term，变成follower
		rf.currentTerm = args.Term
		if rf.state != FOLLOWER{
			rf.state = FOLLOWER
			//fmt.Printf("%d becomes follower3\n", rf.me)
		}
		rf.votedFor = -1
		rf.persist()
	}
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.NextIndex = rf.getLastLogIndex() + 1
		//fmt.Println(reply.ConflictIndex, args.PrevLogIndex, args.PrevLogTerm)
		rf.appendLogChan <- true
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm{
			term := rf.log[args.PrevLogIndex].Term
			//reply.ConflictIndex = rf.getLastLogIndex()

			for i := args.PrevLogIndex-1; i >=0; i-- {
				if rf.log[i].Term != term{
					reply.NextIndex = i+1
					break
				}
			}

			//fmt.Println(reply.ConflictIndex, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
			reply.Success = false
			rf.appendLogChan <- true
			return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	// Append any new entries not already in the log
	if rf.getLastLogIndex() >= args.PrevLogIndex{
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyStateMachine()
	}
	rf.appendLogChan <- true
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getPrevLogIndex(idx int) int{
	return rf.nextIndex[idx]-1
}
func (rf *Raft) getPrevLogTerm(idx int) int{
	prevIdx := rf.getPrevLogIndex(idx)
	return rf.log[prevIdx].Term
}
func min(a int, b int) int{
	if a < b{
		return a
	}
	return b
}
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (rf *Raft) checkIfUpdateCommitIndex(){
	//rf.matchIndex[rf.me] = rf.getLastLogIndex()
	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndexCopy)))
	N := matchIndexCopy[len(matchIndexCopy)/2]
	if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm{
		rf.commitIndex = N
	}
}

func(rf *Raft) applyStateMachine(){
	if(rf.commitIndex > rf.lastApplied){
		for i := rf.lastApplied+1; i <= rf.commitIndex; i++{
			applyMsg := ApplyMsg{
				Index:       i,
				Command:     rf.log[i].Command,
				UseSnapshot: false,
				Snapshot:    nil,
			}
			rf.applyChan <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
	}
}
func (rf *Raft) broadcastLogEntries(){
	for i := range rf.peers {
		if i == rf.me || rf.state != LEADER {
			continue
		}
		go func(i int) {
			reply := &AppendEntriesReply{}
			rf.mu.Lock()
			entries := make([]LogEntry, len(rf.log[rf.nextIndex[i]:]))
			copy(entries, rf.log[rf.nextIndex[i]:])
			appendEntriesMsg := AppendEntriesArgs{
				Term:rf.currentTerm,
				LeaderId:rf.me,
				PrevLogIndex:rf.getPrevLogIndex(i),
				PrevLogTerm:rf.getPrevLogTerm(i),
				Entries:entries,
				LeaderCommit:rf.commitIndex}
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(i, appendEntriesMsg, reply)
			if ok {		// 成功发送
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != LEADER || rf.currentTerm != appendEntriesMsg.Term {	// 状态有变，返回
					return
				}
				if rf.currentTerm < reply.Term {	// 收到更新的term 变回follower
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					return
				}
				if reply.Success{	//通过一致性检查
					//rf.nextIndex[i] = rf.getLastLogIndex() + 1
					//rf.matchIndex[i] = rf.getLastLogIndex()
					if len(entries) > 0 {
						rf.nextIndex[i] = entries[len(entries) - 1].Index + 1
						rf.matchIndex[i] = rf.nextIndex[i] - 1
					}
					rf.checkIfUpdateCommitIndex()
					rf.applyStateMachine()
				}else{	//不一致
					//rf.nextIndex[i] --
					rf.nextIndex[i] = reply.NextIndex
				}
			}
		}(i)

	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == LEADER)
	if(isLeader){
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Index:index, Term:term, Command:command})
		rf.matchIndex[rf.me] = rf.getLastLogIndex()
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.exitChan <- true
}


func (rf *Raft) getLastLogIndex() int{
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int{
	return rf.log[len(rf.log)-1].Term
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

	// Your initialization code here.
	rf.state = FOLLOWER
	//fmt.Printf("%d is initialized as a follower\n", rf.me)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0, Index:0})	//index从1开始

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyChan = applyCh
	rf.exitChan = make(chan bool, 100)
	rf.voteChan = make(chan bool, 100)
	rf.appendLogChan = make(chan bool, 100)
	rf.leaderChan = make(chan bool, 100)

	hearbeatInterval := time.Duration(100) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			select {
			case <-rf.exitChan: return
			default:
			}
			electionTimeout := time.Duration(200 + rand.Intn(300)) * time. Millisecond
			switch rf.state {
			case FOLLOWER:
				select{
				case <- rf.voteChan: // 收到有效投票消息 pass
				case <- rf.appendLogChan: // 收到心跳 pass
				case <- time.After(electionTimeout): // 超过了election timeout，成为候选人
					rf.state = CANDIDATE
					//fmt.Printf("%d becomes candidate\n", rf.me)
				}
			case CANDIDATE:
				go rf.launchElection()	//发起选举
				select{
				case <- rf.leaderChan:		// 1 赢得选举
					rf.mu.Lock()
					rf.state = LEADER
					rf.nextIndex = make([]int, len(peers))
					rf.matchIndex = make([]int, len(peers))
					for i := 0; i < len(rf.nextIndex); i++ {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
					}
					rf.mu.Unlock()
				case <- rf.appendLogChan:	// 2 输了选举，收到别server成为leader后发来的RPC，变回follower
					rf.state = FOLLOWER
				case <-time.After(time.Duration(rand.Intn(500)) * time.Millisecond): //3平局 重新选举
				}
			case LEADER:
				rf.broadcastLogEntries()
				time.Sleep(hearbeatInterval)
			}
		}
	}()

	return rf
}
