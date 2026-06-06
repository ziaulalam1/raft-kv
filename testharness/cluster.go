package testharness

import (
	"fmt"
	"sync"
	"time"

	"github.com/ziaulalam1/raft-kv/raft"
)

// TestCluster runs N Raft nodes in-process with an interceptable
// message transport. This gives tests deterministic control over
// network behavior: partitions, delays, and message drops.
//
// Using real HTTP between nodes would introduce port allocation
// flakiness and non-deterministic timing. The in-process transport
// has the same interface (raft.Transport) as the HTTP transport, so
// the Node code doesn't know the difference.
type TestCluster struct {
	mu        sync.Mutex
	nodes     map[int]*raft.Node
	applied   map[int][]raft.LogEntry // entries applied per node
	network   *PartitionableNetwork
	applyHook func(nodeID int, entry raft.LogEntry) // optional; set via WithApplyHook
}

// ClusterOption configures a TestCluster at construction time.
type ClusterOption func(*TestCluster)

// WithApplyHook registers a function that is called each time a node
// applies a committed entry. Useful for attaching a state machine (e.g.,
// a KVStore) directly to the cluster without polling GetApplied.
// Called after the cluster's internal applied-tracking, outside tc.mu.
func WithApplyHook(hook func(nodeID int, entry raft.LogEntry)) ClusterOption {
	return func(tc *TestCluster) {
		tc.applyHook = hook
	}
}

func NewTestCluster(nodeCount int, opts ...ClusterOption) *TestCluster {
	tc := &TestCluster{
		nodes:   make(map[int]*raft.Node),
		applied: make(map[int][]raft.LogEntry),
		network: NewPartitionableNetwork(),
	}

	// Apply options before creating nodes so hook is set when apply closures capture tc.
	for _, opt := range opts {
		opt(tc)
	}

	ids := make([]int, nodeCount)
	for i := 0; i < nodeCount; i++ {
		ids[i] = i + 1
	}

	for _, id := range ids {
		peers := make([]int, 0, nodeCount-1)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}

		nodeID := id
		transport := &InProcessTransport{
			nodeID:  nodeID,
			cluster: tc,
			network: tc.network,
		}

		applyFunc := func(entry raft.LogEntry) {
			tc.mu.Lock()
			tc.applied[nodeID] = append(tc.applied[nodeID], entry)
			tc.mu.Unlock()
			if tc.applyHook != nil {
				tc.applyHook(nodeID, entry)
			}
		}

		node := raft.NewNode(nodeID, peers, transport, applyFunc, raft.NewMemLog())
		tc.nodes[id] = node
	}

	return tc
}

func (tc *TestCluster) Start() {
	for _, node := range tc.nodes {
		node.Start()
	}
}

func (tc *TestCluster) Stop() {
	for _, node := range tc.nodes {
		node.Stop()
	}
}

// GetNode returns the node with the given ID.
func (tc *TestCluster) GetNode(id int) *raft.Node {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.nodes[id]
}

// GetApplied returns entries applied to a node's state machine.
func (tc *TestCluster) GetApplied(id int) []raft.LogEntry {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	out := make([]raft.LogEntry, len(tc.applied[id]))
	copy(out, tc.applied[id])
	return out
}

// WaitForLeader polls until a leader is elected or timeout expires.
// Returns the leader's node ID or -1 if no leader found.
func (tc *TestCluster) WaitForLeader(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tc.mu.Lock()
		nodesCopy := make(map[int]*raft.Node, len(tc.nodes))
		for id, n := range tc.nodes {
			nodesCopy[id] = n
		}
		tc.mu.Unlock()

		for id, node := range nodesCopy {
			if node.IsDead() {
				continue
			}
			_, state, _ := node.GetState()
			if state == raft.Leader {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return -1
}

// WaitForCommit polls until the given node has commitIndex >= index.
func (tc *TestCluster) WaitForCommit(nodeID, index int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	node := tc.GetNode(nodeID)
	for time.Now().Before(deadline) {
		if node.GetCommitIndex() >= index {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// KillNode stops a node, simulating a crash.
func (tc *TestCluster) KillNode(id int) {
	tc.mu.Lock()
	node, ok := tc.nodes[id]
	tc.mu.Unlock()
	if ok {
		node.Stop()
	}
}

// RestartNode creates a fresh node with the same ID. In a real
// implementation, the node would recover state from its WAL.
// Here, it starts with an empty log (simulating total state loss).
func (tc *TestCluster) RestartNode(id int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	peers := make([]int, 0)
	for pid := range tc.nodes {
		if pid != id {
			peers = append(peers, pid)
		}
	}

	nodeID := id
	transport := &InProcessTransport{
		nodeID:  nodeID,
		cluster: tc,
		network: tc.network,
	}

	applyFunc := func(entry raft.LogEntry) {
		tc.mu.Lock()
		tc.applied[nodeID] = append(tc.applied[nodeID], entry)
		tc.mu.Unlock()
		if tc.applyHook != nil {
			tc.applyHook(nodeID, entry)
		}
	}

	node := raft.NewNode(id, peers, transport, applyFunc, raft.NewMemLog())
	tc.nodes[id] = node
	tc.applied[id] = nil
	node.Start()
}

// Partition creates a network partition between two groups of nodes.
func (tc *TestCluster) Partition(groupA, groupB []int) {
	tc.network.Partition(groupA, groupB)
}

// Heal removes all network partitions.
func (tc *TestCluster) Heal() {
	tc.network.Heal()
}

// Leaders returns all nodes that currently believe they are leader.
// In a healthy cluster this should be exactly 1. During elections or
// partitions, it might temporarily be 0 or >1.
func (tc *TestCluster) Leaders() []int {
	tc.mu.Lock()
	nodesCopy := make(map[int]*raft.Node, len(tc.nodes))
	for id, node := range tc.nodes {
		nodesCopy[id] = node
	}
	tc.mu.Unlock()

	var leaders []int
	for id, node := range nodesCopy {
		if node.IsDead() {
			continue
		}
		_, state, _ := node.GetState()
		if state == raft.Leader {
			leaders = append(leaders, id)
		}
	}
	return leaders
}

// --- In-process transport ---

// InProcessTransport routes RPCs through the TestCluster, respecting
// network partitions. No HTTP, no ports, fully deterministic.
type InProcessTransport struct {
	nodeID  int
	cluster *TestCluster
	network *PartitionableNetwork
}

func (tr *InProcessTransport) SendRequestVote(peerID int, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	if tr.network.IsPartitioned(tr.nodeID, peerID) {
		return raft.RequestVoteReply{}, fmt.Errorf("network partition between %d and %d", tr.nodeID, peerID)
	}

	peer := tr.cluster.GetNode(peerID)
	if peer == nil || peer.IsDead() {
		return raft.RequestVoteReply{}, fmt.Errorf("node %d is dead", peerID)
	}

	// Deterministic per-peer latency, not random: same interleaving on
	// every run makes race-detector failures reproducible.
	time.Sleep(time.Duration(1+peerID) * time.Millisecond)

	reply := peer.HandleRequestVote(args)
	return reply, nil
}

func (tr *InProcessTransport) SendPreVote(peerID int, args raft.PreVoteArgs) (raft.PreVoteReply, error) {
	if tr.network.IsPartitioned(tr.nodeID, peerID) {
		return raft.PreVoteReply{}, fmt.Errorf("network partition between %d and %d", tr.nodeID, peerID)
	}

	peer := tr.cluster.GetNode(peerID)
	if peer == nil || peer.IsDead() {
		return raft.PreVoteReply{}, fmt.Errorf("node %d is dead", peerID)
	}

	// Same deterministic latency as the other RPCs — keeps interleavings
	// reproducible for the race detector.
	time.Sleep(time.Duration(1+peerID) * time.Millisecond)

	reply := peer.HandlePreVote(args)
	return reply, nil
}

func (tr *InProcessTransport) SendAppendEntries(peerID int, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	if tr.network.IsPartitioned(tr.nodeID, peerID) {
		return raft.AppendEntriesReply{}, fmt.Errorf("network partition between %d and %d", tr.nodeID, peerID)
	}

	peer := tr.cluster.GetNode(peerID)
	if peer == nil || peer.IsDead() {
		return raft.AppendEntriesReply{}, fmt.Errorf("node %d is dead", peerID)
	}

	// Same deterministic latency as SendRequestVote.
	time.Sleep(time.Duration(1+peerID) * time.Millisecond)

	reply := peer.HandleAppendEntries(args)
	return reply, nil
}

