package testharness

import "sync"

// PartitionableNetwork models network partitions between nodes.
// A partition is bidirectional: if A can't reach B, B can't reach A.
//
// Implementation: a set of blocked (nodeA, nodeB) pairs. Checking
// partition status is O(1) via map lookup. This is simpler than
// modeling network topology, and sufficient for testing Raft's
// partition behavior.
type PartitionableNetwork struct {
	mu      sync.RWMutex
	blocked map[[2]int]bool
}

func NewPartitionableNetwork() *PartitionableNetwork {
	return &PartitionableNetwork{
		blocked: make(map[[2]int]bool),
	}
}

// Partition blocks communication between every node in groupA and
// every node in groupB. Nodes within the same group can still
// communicate.
func (pn *PartitionableNetwork) Partition(groupA, groupB []int) {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	for _, a := range groupA {
		for _, b := range groupB {
			pn.blocked[pairKey(a, b)] = true
			pn.blocked[pairKey(b, a)] = true
		}
	}
}

// Heal removes all partitions.
func (pn *PartitionableNetwork) Heal() {
	pn.mu.Lock()
	defer pn.mu.Unlock()
	pn.blocked = make(map[[2]int]bool)
}

// IsPartitioned returns true if nodeA cannot reach nodeB.
func (pn *PartitionableNetwork) IsPartitioned(nodeA, nodeB int) bool {
	pn.mu.RLock()
	defer pn.mu.RUnlock()
	return pn.blocked[pairKey(nodeA, nodeB)]
}

func pairKey(a, b int) [2]int {
	return [2]int{a, b}
}
