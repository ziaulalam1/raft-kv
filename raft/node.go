package raft

import (
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// Timing constants. The key relationship is:
//   broadcastTime << electionTimeout << MTBF
// where broadcastTime is the heartbeat interval. Section 5.6 of
// the paper requires electionTimeout >> broadcastTime so followers
// don't start spurious elections during normal operation.
const (
	heartbeatInterval  = 50 * time.Millisecond // must be << electionTimeoutMin
	electionTimeoutMin = 300                    // ms
	electionTimeoutMax = 500                    // ms; wider spread reduces split-vote probability
	rpcTimeout         = 75 * time.Millisecond  // per-RPC deadline
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// ApplyFunc is called when a committed entry is applied to the state
// machine. The node does not interpret commands; it passes them through
// to whatever state machine sits on top (e.g., the KV store).
type ApplyFunc func(entry LogEntry)

// Node is a single Raft participant. Handles leader election, log replication,
// and commit advancement. Single mutex protects all mutable state; critical
// sections are short so contention is not an issue at this scale.
type Node struct {
	mu sync.Mutex

	// Persistent state (in-memory only; no WAL).
	id          int
	currentTerm int
	votedFor    int // -1 means no vote in current term
	log         LogStore

	// Volatile state.
	state       State
	commitIndex int
	lastApplied int
	leaderId    int // who we think the leader is, -1 if unknown

	// Leader-only volatile state (reinitialized after election).
	nextIndex  map[int]int // for each peer: next log index to send
	matchIndex map[int]int // for each peer: highest log index known replicated

	// Peers and transport.
	peers     []int // IDs of other nodes in the cluster
	transport Transport

	// Timer management.
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Wall-clock of last AppendEntries from a leader. HandlePreVote uses
	// this for §9.6 leader stickiness: deny pre-votes if a leader has been
	// heard from within electionTimeoutMin. Zero value (startup) lets the
	// first election round proceed.
	lastLeaderContact time.Time

	// Callback for state machine application.
	applyFunc ApplyFunc

	// Shutdown.
	stopCh chan struct{}
	dead   bool

	// Structured logger; per-node "node" attribute is set in NewNode so
	// every line carries node identity without callsite plumbing.
	logger *slog.Logger
}

func NewNode(id int, peers []int, transport Transport, applyFunc ApplyFunc, log LogStore) *Node {
	n := &Node{
		id:          id,
		currentTerm: 0,
		votedFor:    -1,
		log:         log,
		state:       Follower,
		commitIndex: 0,
		lastApplied: 0,
		leaderId:    -1,
		peers:       peers,
		transport:   transport,
		applyFunc:   applyFunc,
		stopCh:      make(chan struct{}),
		logger:      slog.With("node", id),
	}
	return n
}

func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()
}

// Safe to call multiple times (idempotent).
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dead {
		return // already stopped
	}
	n.dead = true
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	close(n.stopCh)
}

func (n *Node) IsDead() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dead
}

// leaderId is -1 when the leader is unknown (no leader seen this term).
func (n *Node) GetState() (int, State, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.state, n.leaderId
}

func (n *Node) GetID() int {
	return n.id
}

func (n *Node) GetLog() LogStore {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log
}

func (n *Node) GetCommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func randomElectionTimeout() time.Duration {
	ms := electionTimeoutMin + rand.Intn(electionTimeoutMax-electionTimeoutMin)
	return time.Duration(ms) * time.Millisecond
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := randomElectionTimeout()
	n.electionTimer = time.AfterFunc(timeout, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.dead {
			return
		}
		if n.state != Leader {
			n.startPreVote()
		}
	})
}

// startPreVote runs the §9.6 pre-vote round before any real election.
// Sends RPCs with proposedTerm = currentTerm+1 but does not adopt it
// locally; currentTerm only advances if a majority grants, in which
// case startElection takes over. Failed rounds leave currentTerm
// unchanged, so a partitioned node never bumps its term on a timeout
// and can't force a step-down when it rejoins.
//
// Must be called with n.mu held.
func (n *Node) startPreVote() {
	savedTerm := n.currentTerm
	proposedTerm := savedTerm + 1
	lastLogIndex := n.log.LastIndex()
	lastLogTerm := n.log.LastTerm()
	grants := 1 // peer counts self
	needed := (len(n.peers)+1)/2 + 1

	n.logger.Info("starting pre-vote", "proposedTerm", proposedTerm)

	// Reset up front; startElection resets again on majority, which is fine.
	n.resetElectionTimer()

	for _, peerID := range n.peers {
		go func(peer int) {
			args := PreVoteArgs{
				Term:         proposedTerm,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := n.transport.SendPreVote(peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// Stale: term advanced or state changed while we waited.
			if n.dead || n.currentTerm != savedTerm || n.state == Leader {
				return
			}

			if reply.VoteGranted {
				grants++
				if grants >= needed {
					// Majority — promote. Sentinel prevents a slow reply
					// from re-firing startElection.
					grants = -1
					n.startElection()
				}
			}
		}(peerID)
	}
}

// startElection transitions to Candidate and requests votes from all peers.
// Must be called with n.mu held.
func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id // vote for self
	n.leaderId = -1
	savedTerm := n.currentTerm
	lastLogIndex := n.log.LastIndex()
	lastLogTerm := n.log.LastTerm()
	votesReceived := 1 // self-vote
	votesNeeded := (len(n.peers)+1)/2 + 1

	n.logger.Info("starting election", "term", savedTerm)

	n.resetElectionTimer()

	// Send RequestVote RPCs in parallel.
	for _, peerID := range n.peers {
		go func(peer int) {
			args := RequestVoteArgs{
				Term:         savedTerm,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := n.transport.SendRequestVote(peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.dead || n.currentTerm != savedTerm || n.state != Candidate {
				return // stale response
			}

			if reply.Term > savedTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if reply.VoteGranted {
				votesReceived++
				if votesReceived >= votesNeeded {
					n.becomeLeader()
				}
			}
		}(peerID)
	}
}

// becomeFollower transitions to Follower state. Called when we discover
// a higher term from any RPC response or request. votedFor must reset
// on every term advance — a stale vote carried into a new term breaks
// election safety across terms even if per-term uniqueness holds.
func (n *Node) becomeFollower(term int) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.leaderId = -1
	n.resetElectionTimer()
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
		n.heartbeatTimer = nil
	}
}

// becomeLeader transitions to Leader. Immediately sends heartbeats to
// establish authority (§5.2).
func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderId = n.id
	n.nextIndex = make(map[int]int)
	n.matchIndex = make(map[int]int)
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.log.LastIndex() + 1
		n.matchIndex[peer] = 0
	}

	n.logger.Info("became leader", "term", n.currentTerm)

	// Stop election timer; leaders don't need it.
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	// Start heartbeat loop.
	n.sendHeartbeats()
	n.heartbeatTimer = time.AfterFunc(heartbeatInterval, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.dead || n.state != Leader {
			return
		}
		n.sendHeartbeats()
		// Reschedule.
		n.heartbeatTimer.Reset(heartbeatInterval)
	})
}

// sendHeartbeats sends AppendEntries to all peers. Must be called with n.mu held.
func (n *Node) sendHeartbeats() {
	savedTerm := n.currentTerm
	for _, peerID := range n.peers {
		go func(peer int) {
			n.mu.Lock()
			if n.dead || n.state != Leader || n.currentTerm != savedTerm {
				n.mu.Unlock()
				return
			}
			nextIdx := n.nextIndex[peer]
			prevLogIndex := nextIdx - 1
			prevLogTerm := n.log.TermAt(prevLogIndex)
			entries := n.log.Slice(nextIdx)
			commitIndex := n.commitIndex
			n.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         savedTerm,
				LeaderId:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}

			reply, err := n.transport.SendAppendEntries(peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.dead || n.state != Leader || n.currentTerm != savedTerm {
				return
			}

			if reply.Term > savedTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if reply.Success {
				// Update nextIndex and matchIndex for this peer.
				if len(entries) > 0 {
					newMatchIndex := entries[len(entries)-1].Index
					n.matchIndex[peer] = newMatchIndex
					n.nextIndex[peer] = newMatchIndex + 1
					n.advanceCommitIndex()
				}
			} else {
				// Conflict-term optimization (§5.3): skip back a full term.
				if reply.ConflictTerm < 0 {
					// Follower's log is shorter; jump to where it needs entries.
					n.nextIndex[peer] = reply.ConflictIndex
				} else {
					// Find the last index in our log with ConflictTerm.
					idx := -1
					for i := n.log.LastIndex(); i >= 1; i-- {
						et := n.log.TermAt(i)
						if et == reply.ConflictTerm {
							idx = i
							break
						}
						if et < reply.ConflictTerm {
							break
						}
					}
					if idx >= 0 {
						// We have entries with ConflictTerm; set nextIndex just past them.
						n.nextIndex[peer] = idx + 1
					} else {
						// We don't have ConflictTerm at all; jump to follower's conflict point.
						n.nextIndex[peer] = reply.ConflictIndex
					}
				}
				if n.nextIndex[peer] < 1 {
					n.nextIndex[peer] = 1
				}
			}
		}(peerID)
	}
}

// advanceCommitIndex checks if there's an N > commitIndex such that
// a majority of matchIndex[i] >= N and log[N].term == currentTerm.
// The term check is critical: a leader can only commit entries from
// its own term (section 5.4.2). Entries from previous terms get
// committed indirectly when a current-term entry after them is committed.
func (n *Node) advanceCommitIndex() {
	for idx := n.commitIndex + 1; idx <= n.log.LastIndex(); idx++ {
		if n.log.TermAt(idx) != n.currentTerm {
			continue
		}
		replicatedCount := 1 // count self
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				replicatedCount++
			}
		}
		if replicatedCount > (len(n.peers)+1)/2 {
			n.commitIndex = idx
			n.applyCommitted()
		}
	}
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry, err := n.log.EntryAt(n.lastApplied)
		if err != nil {
			n.logger.Error("error reading log", "index", n.lastApplied, "err", err)
			return
		}
		if n.applyFunc != nil {
			n.applyFunc(entry)
		}
	}
}

// --- RPC Handlers ---

// HandlePreVote processes a PreVote RPC. Does not mutate currentTerm,
// votedFor, or state — that's what stops a partitioned node with a
// stale-but-high term from forcing a step-down here. Grants only if:
//  1. args.Term >= currentTerm (lower means stale)
//  2. lastLeaderContact is older than electionTimeoutMin (no live leader)
//  3. candidate's log is at least as up-to-date as ours (§5.4.1)
func (n *Node) HandlePreVote(args PreVoteArgs) PreVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := PreVoteReply{Term: n.currentTerm, VoteGranted: false}

	if n.dead {
		return reply
	}

	if args.Term < n.currentTerm {
		return reply
	}

	// Leader stickiness. Zero-valued lastLeaderContact (never heard from
	// any leader) skips this check, which is correct for cluster startup.
	if !n.lastLeaderContact.IsZero() {
		minTimeout := time.Duration(electionTimeoutMin) * time.Millisecond
		if time.Since(n.lastLeaderContact) < minTimeout {
			return reply
		}
	}

	lastLogTerm := n.log.LastTerm()
	lastLogIndex := n.log.LastIndex()
	logOk := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if !logOk {
		return reply
	}

	reply.VoteGranted = true
	return reply
}

// HandleRequestVote processes a RequestVote RPC from a candidate.
// Section 5.4.1: grant vote only if candidate's log is at least as
// up-to-date as ours, and we haven't voted for someone else this term.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.dead {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	reply := RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

	if args.Term < n.currentTerm {
		return reply
	}

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply.Term = n.currentTerm

	// Check if we can vote for this candidate.
	if n.votedFor == -1 || n.votedFor == args.CandidateId {
		// Election restriction (section 5.4.1): candidate's log must be
		// at least as up-to-date as ours. "Up-to-date" means: higher last
		// term wins; if terms are equal, longer log wins.
		lastLogTerm := n.log.LastTerm()
		lastLogIndex := n.log.LastIndex()
		logOk := args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

		if logOk {
			n.votedFor = args.CandidateId
			reply.VoteGranted = true
			n.resetElectionTimer() // reset so we don't start our own election
		}
	}

	return reply
}

// HandleAppendEntries processes an AppendEntries RPC from the leader.
// This serves double duty: heartbeat (empty entries) and log replication
// (with entries). Section 5.3.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.dead {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	reply := AppendEntriesReply{Term: n.currentTerm, Success: false}

	if args.Term < n.currentTerm {
		return reply
	}

	// Valid leader for this term. Reset election timer.
	if args.Term > n.currentTerm || n.state != Follower {
		n.becomeFollower(args.Term)
	}
	n.leaderId = args.LeaderId
	// Stamp before the log-consistency check — leader is alive whether or
	// not our log agrees, and HandlePreVote uses this for stickiness.
	n.lastLeaderContact = time.Now()
	n.resetElectionTimer()

	reply.Term = n.currentTerm

	// Log consistency check. On failure, populate conflict info (§5.3) so the
	// leader can skip back a full term instead of decrementing nextIndex by one.
	if !n.log.MatchCheck(args.PrevLogIndex, args.PrevLogTerm) {
		if args.PrevLogIndex > n.log.LastIndex() {
			reply.ConflictTerm = -1
			reply.ConflictIndex = n.log.LastIndex() + 1
		} else {
			reply.ConflictTerm = n.log.TermAt(args.PrevLogIndex)
			ci := n.log.FirstIndexOfTerm(reply.ConflictTerm)
			if ci < 0 {
				ci = args.PrevLogIndex
			}
			reply.ConflictIndex = ci
		}
		return reply
	}

	// Append entries (handles conflicts via truncation).
	if len(args.Entries) > 0 {
		n.log.AppendEntries(args.PrevLogIndex, args.Entries)
	}

	// Advance commit index if leader is ahead.
	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := n.log.LastIndex()
		if args.LeaderCommit < lastNewIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		n.applyCommitted()
	}

	reply.Success = true
	return reply
}

// Submit proposes a command. Returns (index, term, true) on the leader;
// followers return (-1, -1, false) and the caller can redirect via GetState.
func (n *Node) Submit(command []byte) (int, int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.dead || n.state != Leader {
		return -1, -1, false
	}

	index := n.log.Append(n.currentTerm, command)
	n.logger.Info("leader accepted entry", "index", index, "term", n.currentTerm)

	// Trigger immediate replication instead of waiting for next heartbeat.
	n.sendHeartbeats()

	return index, n.currentTerm, true
}
