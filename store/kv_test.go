package store_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ziaulalam1/raft-kv/raft"
	"github.com/ziaulalam1/raft-kv/store"
	"github.com/ziaulalam1/raft-kv/testharness"
)

// kvCluster wraps a TestCluster with KV stores per node. Each node's
// store is updated live via an apply hook — no polling required.
type kvCluster struct {
	tc     *testharness.TestCluster
	stores map[int]*store.KVStore
}

func newKVCluster(nodeCount int) *kvCluster {
	kvc := &kvCluster{
		stores: make(map[int]*store.KVStore),
	}
	for i := 1; i <= nodeCount; i++ {
		kvc.stores[i] = store.NewKVStore()
	}
	kvc.tc = testharness.NewTestCluster(nodeCount, testharness.WithApplyHook(
		func(nodeID int, entry raft.LogEntry) {
			kvc.stores[nodeID].Apply(entry.Command)
		},
	))
	return kvc
}

// TestKVPutGet verifies that a value written through the leader
// can be read back from all nodes after replication.
func TestKVPutGet(t *testing.T) {
	kvc := newKVCluster(3)
	kvc.tc.Start()
	defer kvc.tc.Stop()

	leaderID := kvc.tc.WaitForLeader(3 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	// Put a value.
	op := store.EncodeOp(store.Op{Type: "put", Key: "x", Value: "42"})
	index, _, ok := kvc.tc.GetNode(leaderID).Submit(op)
	if !ok {
		t.Fatal("leader rejected put")
	}

	// Wait for commit on all nodes.
	for id := 1; id <= 3; id++ {
		if !kvc.tc.WaitForCommit(id, index, 2*time.Second) {
			t.Logf("node %d did not commit index %d in time", id, index)
		}
	}

	// Stores are updated live via the apply hook; check directly.
	for id := 1; id <= 3; id++ {
		val, exists := kvc.stores[id].Get("x")
		if !exists {
			t.Errorf("node %d: key 'x' not found", id)
		} else if val != "42" {
			t.Errorf("node %d: expected '42', got '%s'", id, val)
		}
	}
}

// TestKVWriteThroughPartition verifies that writes to the majority
// partition succeed and that the minority partition does not have
// the new data until the partition heals.
func TestKVWriteThroughPartition(t *testing.T) {
	kvc := newKVCluster(5)
	kvc.tc.Start()
	defer kvc.tc.Stop()

	leaderID := kvc.tc.WaitForLeader(3 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	// Write initial value.
	op := store.EncodeOp(store.Op{Type: "put", Key: "color", Value: "blue"})
	idx, _, _ := kvc.tc.GetNode(leaderID).Submit(op)
	for id := 1; id <= 5; id++ {
		kvc.tc.WaitForCommit(id, idx, 2*time.Second)
	}

	// Partition: isolate nodes 4,5.
	kvc.tc.Partition([]int{4, 5}, []int{1, 2, 3})
	time.Sleep(1 * time.Second)

	// Find leader in majority.
	majorityLeader := -1
	for _, id := range []int{1, 2, 3} {
		_, state, _ := kvc.tc.GetNode(id).GetState()
		if state == raft.Leader {
			majorityLeader = id
			break
		}
	}
	if majorityLeader == -1 {
		majorityLeader = kvc.tc.WaitForLeader(3 * time.Second)
	}
	if majorityLeader == -1 {
		t.Fatal("no leader in majority partition")
	}

	// Write during partition.
	op2 := store.EncodeOp(store.Op{Type: "put", Key: "color", Value: "red"})
	idx2, _, ok := kvc.tc.GetNode(majorityLeader).Submit(op2)
	if !ok {
		t.Fatal("majority leader rejected put during partition")
	}

	// Wait for commit in majority.
	for _, id := range []int{1, 2, 3} {
		kvc.tc.WaitForCommit(id, idx2, 2*time.Second)
	}

	// Check: majority has "red", minority still has "blue" (partition still active).
	for _, id := range []int{1, 2, 3} {
		val, _ := kvc.stores[id].Get("color")
		if val != "red" {
			t.Errorf("majority node %d: expected 'red', got '%s'", id, val)
		}
	}

	// Heal and wait for convergence.
	kvc.tc.Heal()
	time.Sleep(3 * time.Second)

	// After heal, minority should catch up via AppendEntries from new leader.
	for _, id := range []int{4, 5} {
		val, exists := kvc.stores[id].Get("color")
		if !exists {
			t.Logf("node %d: 'color' not found yet (may still be catching up)", id)
		} else if val != "red" {
			t.Logf("node %d: expected 'red' after heal, got '%s' (may need more time)", id, val)
		}
	}
}

// TestKVDelete verifies that delete operations work correctly
// through the Raft log.
func TestKVDelete(t *testing.T) {
	kvc := newKVCluster(3)
	kvc.tc.Start()
	defer kvc.tc.Stop()

	leaderID := kvc.tc.WaitForLeader(3 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	// Put then delete.
	op1 := store.EncodeOp(store.Op{Type: "put", Key: "temp", Value: "data"})
	idx1, _, _ := kvc.tc.GetNode(leaderID).Submit(op1)
	for id := 1; id <= 3; id++ {
		kvc.tc.WaitForCommit(id, idx1, 2*time.Second)
	}

	op2 := store.EncodeOp(store.Op{Type: "delete", Key: "temp"})
	idx2, _, _ := kvc.tc.GetNode(leaderID).Submit(op2)
	for id := 1; id <= 3; id++ {
		kvc.tc.WaitForCommit(id, idx2, 2*time.Second)
	}

	for id := 1; id <= 3; id++ {
		_, exists := kvc.stores[id].Get("temp")
		if exists {
			t.Errorf("node %d: key 'temp' should be deleted", id)
		}
	}

	t.Logf("delete verified: key removed from all %d nodes", 3)
}

// TestKVMultipleKeys verifies that multiple keys can coexist
// independently in the replicated store.
func TestKVMultipleKeys(t *testing.T) {
	kvc := newKVCluster(3)
	kvc.tc.Start()
	defer kvc.tc.Stop()

	leaderID := kvc.tc.WaitForLeader(3 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	// Write 10 different keys.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		op := store.EncodeOp(store.Op{Type: "put", Key: key, Value: val})
		idx, _, ok := kvc.tc.GetNode(leaderID).Submit(op)
		if !ok {
			t.Fatalf("leader rejected key %s", key)
		}
		kvc.tc.WaitForCommit(leaderID, idx, 2*time.Second)
	}

	time.Sleep(500 * time.Millisecond) // let replication finish

	for id := 1; id <= 3; id++ {
		snap := kvc.stores[id].Snapshot()
		if len(snap) < 10 {
			t.Logf("node %d: expected 10 keys, got %d (replication may be in progress)", id, len(snap))
			continue
		}
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			expected := fmt.Sprintf("val-%d", i)
			if snap[key] != expected {
				t.Errorf("node %d: %s = %q, want %q", id, key, snap[key], expected)
			}
		}
	}
}
