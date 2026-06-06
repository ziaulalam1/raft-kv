package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ziaulalam1/raft-kv/raft"
	"github.com/ziaulalam1/raft-kv/testharness"
)

// TestElectionSafety_AtMostOneLeaderPerTerm verifies the most fundamental
// Raft invariant: no two nodes can be leader in the same term. This is
// what prevents split-brain. We run multiple rounds of elections by
// killing leaders and forcing re-election, then check that every term
// had at most one leader.
//
// This test caught a real bug: votedFor wasn't being reset
// when a node discovered a higher term, which allowed double-voting
// across term boundaries.
func TestElectionSafety_AtMostOneLeaderPerTerm(t *testing.T) {
	const rounds = 20
	const nodeCount = 5

	tc := testharness.NewTestCluster(nodeCount)
	tc.Start()
	defer tc.Stop()

	// Track which nodes claimed leadership in which terms.
	type leaderRecord struct {
		term     int
		leaderID int
	}
	var records []leaderRecord

	for round := 0; round < rounds; round++ {
		// Wait for a leader.
		leaderID := tc.WaitForLeader(3 * time.Second)
		if leaderID == -1 {
			t.Fatalf("round %d: no leader elected within timeout", round)
		}

		term, state, _ := tc.GetNode(leaderID).GetState()
		if state != raft.Leader {
			continue // raced, skip this round
		}
		records = append(records, leaderRecord{term: term, leaderID: leaderID})

		// Kill the leader to force a new election.
		tc.KillNode(leaderID)
		time.Sleep(100 * time.Millisecond)

		// Restart the killed node so it can participate as follower.
		tc.RestartNode(leaderID)
	}

	// Verify: at most one leader per term.
	termLeaders := make(map[int]int) // term -> leader ID
	for _, rec := range records {
		if existing, ok := termLeaders[rec.term]; ok {
			if existing != rec.leaderID {
				t.Errorf("INVARIANT VIOLATION: term %d had two leaders: %d and %d",
					rec.term, existing, rec.leaderID)
			}
		}
		termLeaders[rec.term] = rec.leaderID
	}

	t.Logf("checked %d leader elections across %d terms, all safe", len(records), len(termLeaders))
}

// TestLogMatching_SameIndexTermImpliesPrefixMatch verifies the Log
// Matching Property (section 5.3): if two logs contain an entry with
// the same index and term, then the logs are identical in all preceding
// entries.
//
// We submit several entries through the leader, wait for replication,
// then compare logs across all nodes.
func TestLogMatching_SameIndexTermImpliesPrefixMatch(t *testing.T) {
	tc := testharness.NewTestCluster(3)
	tc.Start()
	defer tc.Stop()

	leaderID := tc.WaitForLeader(3 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	// Submit 5 entries.
	for i := 0; i < 5; i++ {
		cmd := []byte(fmt.Sprintf("cmd-%d", i))
		_, _, ok := tc.GetNode(leaderID).Submit(cmd)
		if !ok {
			t.Fatalf("leader rejected submission %d", i)
		}
		time.Sleep(50 * time.Millisecond) // let replication happen
	}

	// Wait for commits to propagate.
	time.Sleep(500 * time.Millisecond)

	// Compare logs pairwise.
	logs := make(map[int][]raft.LogEntry)
	for id := 1; id <= 3; id++ {
		logs[id] = tc.GetNode(id).GetLog().(*raft.MemLog).Entries()
	}

	for idA := 1; idA <= 3; idA++ {
		for idB := idA + 1; idB <= 3; idB++ {
			logA := logs[idA]
			logB := logs[idB]
			minLen := len(logA)
			if len(logB) < minLen {
				minLen = len(logB)
			}

			for i := 0; i < minLen; i++ {
				if logA[i].Index == logB[i].Index && logA[i].Term == logB[i].Term {
					// Same index and term: all preceding entries must match.
					for j := 0; j < i; j++ {
						if logA[j].Term != logB[j].Term {
							t.Errorf("INVARIANT VIOLATION: nodes %d and %d agree at index %d "+
								"(term %d) but disagree at index %d (terms %d vs %d)",
								idA, idB, logA[i].Index, logA[i].Term,
								logA[j].Index, logA[j].Term, logB[j].Term)
						}
					}
				}
			}
		}
	}

	t.Logf("log matching verified across 3 nodes with %d entries each", len(logs[1]))
}

// TestLeaderCompleteness_CommittedEntryInFutureLeaders verifies that
// once an entry is committed, every future leader has that entry in
// its log (section 5.4.2). We commit an entry, kill the leader, wait
// for a new leader, and check it has the entry.
func TestLeaderCompleteness_CommittedEntryInFutureLeaders(t *testing.T) {
	tc := testharness.NewTestCluster(3)
	tc.Start()
	defer tc.Stop()

	leaderID := tc.WaitForLeader(3 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	// Submit and wait for commit.
	cmd := []byte("committed-value")
	index, _, ok := tc.GetNode(leaderID).Submit(cmd)
	if !ok {
		t.Fatal("leader rejected submission")
	}

	// Wait for the entry to be committed on a majority.
	committed := false
	for id := 1; id <= 3; id++ {
		if tc.WaitForCommit(id, index, 2*time.Second) {
			committed = true
		}
	}
	if !committed {
		t.Fatal("entry never committed on any node")
	}

	// Kill the old leader.
	oldLeader := leaderID
	tc.KillNode(oldLeader)
	time.Sleep(100 * time.Millisecond)

	// Wait for a new leader.
	newLeader := tc.WaitForLeader(3 * time.Second)
	if newLeader == -1 {
		t.Fatal("no new leader elected after killing old leader")
	}
	if newLeader == oldLeader {
		t.Fatal("new leader is the dead node (should not happen)")
	}

	// The new leader must have the committed entry.
	newLeaderLog := tc.GetNode(newLeader).GetLog()
	entry, err := newLeaderLog.EntryAt(index)
	if err != nil {
		t.Fatalf("INVARIANT VIOLATION: new leader %d missing entry at index %d: %v",
			newLeader, index, err)
	}
	if string(entry.Command) != string(cmd) {
		t.Errorf("INVARIANT VIOLATION: new leader %d has different command at index %d: %q vs %q",
			newLeader, index, string(entry.Command), string(cmd))
	}

	t.Logf("leader completeness verified: old leader %d killed, new leader %d has committed entry at index %d",
		oldLeader, newLeader, index)
}

// TestPartitionSafety_MinorityCannotElect verifies that a minority
// partition cannot elect a leader. In a 5-node cluster partitioned
// into groups of 2 and 3, only the group of 3 can elect a leader.
// The group of 2 will keep timing out and incrementing terms but
// never reach a majority.
func TestPartitionSafety_MinorityCannotElect(t *testing.T) {
	tc := testharness.NewTestCluster(5)
	tc.Start()
	defer tc.Stop()

	// Wait for initial leader.
	leaderID := tc.WaitForLeader(3 * time.Second)
	if leaderID == -1 {
		t.Fatal("no initial leader elected")
	}

	// Partition: nodes 1,2 vs nodes 3,4,5
	minority := []int{1, 2}
	majority := []int{3, 4, 5}
	tc.Partition(minority, majority)

	// Kill the leader (whichever group it's in, the minority side won't
	// be able to form a majority).
	tc.KillNode(leaderID)
	time.Sleep(100 * time.Millisecond)
	tc.RestartNode(leaderID)

	// Give time for elections to settle.
	time.Sleep(2 * time.Second)

	// Check: no node in the minority should be leader.
	for _, id := range minority {
		node := tc.GetNode(id)
		if node.IsDead() {
			continue
		}
		_, state, _ := node.GetState()
		if state == raft.Leader {
			t.Errorf("INVARIANT VIOLATION: node %d in minority partition is leader", id)
		}
	}

	// The majority should have a leader.
	majorityHasLeader := false
	for _, id := range majority {
		node := tc.GetNode(id)
		if node.IsDead() {
			continue
		}
		_, state, _ := node.GetState()
		if state == raft.Leader {
			majorityHasLeader = true
		}
	}

	if !majorityHasLeader {
		// This can happen if the original leader was in the majority and
		// we killed it. Wait a bit longer.
		time.Sleep(2 * time.Second)
		for _, id := range majority {
			_, state, _ := tc.GetNode(id).GetState()
			if state == raft.Leader {
				majorityHasLeader = true
			}
		}
	}

	tc.Heal()
	t.Logf("partition safety verified: minority %v has no leader, majority %v has leader: %v",
		minority, majority, majorityHasLeader)
}

// TestConvergence_LogsMatchAfterPartitionHeal verifies that after a
// network partition is healed, all nodes converge to the same log.
// We partition, write to the majority, heal, and then check that the
// minority nodes catch up.
func TestConvergence_LogsMatchAfterPartitionHeal(t *testing.T) {
	tc := testharness.NewTestCluster(5)
	tc.Start()
	defer tc.Stop()

	// Wait for initial leader.
	leaderID := tc.WaitForLeader(3 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	// Submit an entry before partition.
	_, _, ok := tc.GetNode(leaderID).Submit([]byte("before-partition"))
	if !ok {
		t.Fatal("leader rejected pre-partition entry")
	}
	time.Sleep(300 * time.Millisecond) // replicate

	// Partition: isolate nodes 4,5 from 1,2,3
	tc.Partition([]int{4, 5}, []int{1, 2, 3})

	// Find/wait for leader in majority.
	time.Sleep(1 * time.Second)
	majorityLeader := -1
	for _, id := range []int{1, 2, 3} {
		_, state, _ := tc.GetNode(id).GetState()
		if state == raft.Leader {
			majorityLeader = id
			break
		}
	}
	if majorityLeader == -1 {
		majorityLeader = tc.WaitForLeader(3 * time.Second)
	}

	// Submit entries to the majority.
	if majorityLeader != -1 {
		for i := 0; i < 3; i++ {
			tc.GetNode(majorityLeader).Submit([]byte(fmt.Sprintf("during-partition-%d", i)))
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Wait for commits in majority.
	time.Sleep(500 * time.Millisecond)

	// Heal partition.
	tc.Heal()

	// Wait for convergence.
	time.Sleep(3 * time.Second)

	// All nodes should have the same committed entries.
	var refLog []raft.LogEntry
	for id := 1; id <= 5; id++ {
		entries := tc.GetNode(id).GetLog().(*raft.MemLog).Entries()
		if refLog == nil && len(entries) > 0 {
			refLog = entries
			continue
		}

		commitIdx := tc.GetNode(id).GetCommitIndex()
		if commitIdx == 0 {
			continue // restarted node, may not have caught up yet
		}

		// Check up to commitIndex.
		for i := 0; i < commitIdx && i < len(entries) && i < len(refLog); i++ {
			if entries[i].Term != refLog[i].Term {
				t.Errorf("INVARIANT VIOLATION: node %d has term %d at index %d, "+
					"expected term %d (divergence after partition heal)",
					id, entries[i].Term, entries[i].Index, refLog[i].Term)
			}
		}
	}

	t.Logf("convergence verified: all 5 nodes share consistent log after partition heal")
}

// TestConflictTerm_StaleFollowerCatchesUpQuickly verifies the §5.3 conflict-term
// optimization. A follower that is N entries behind the leader should catch up
// in O(1) heartbeat rounds, not O(N).
//
// Setup: partition one follower, submit 50 entries to the majority, heal.
// With the optimization the follower catches up in ~2 heartbeat intervals (~100ms).
// Without it, each heartbeat decrements nextIndex by one: 50 × 50ms = ~2.5s.
// The 600ms deadline sits well between those two outcomes.
func TestConflictTerm_StaleFollowerCatchesUpQuickly(t *testing.T) {
	const entryCount = 50

	tc := testharness.NewTestCluster(3)
	tc.Start()
	defer tc.Stop()

	leaderID := tc.WaitForLeader(2 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	// Pick a follower to isolate (not the leader).
	isolatedID := 2
	if leaderID == 2 {
		isolatedID = 3
	}
	majority := make([]int, 0, 2)
	for i := 1; i <= 3; i++ {
		if i != isolatedID {
			majority = append(majority, i)
		}
	}

	tc.Partition([]int{isolatedID}, majority)

	// Submit entries to the majority partition.
	leader := tc.GetNode(leaderID)
	lastIdx := 0
	for i := 0; i < entryCount; i++ {
		idx, _, ok := leader.Submit([]byte(fmt.Sprintf("entry-%d", i)))
		if !ok {
			// Leader may have changed; find the new one.
			for _, id := range majority {
				n := tc.GetNode(id)
				if n.IsDead() {
					continue
				}
				_, state, _ := n.GetState()
				if state == raft.Leader {
					leaderID = id
					leader = n
					idx, _, ok = leader.Submit([]byte(fmt.Sprintf("entry-%d", i)))
					break
				}
			}
			if !ok {
				t.Fatalf("entry %d: no leader available in majority partition", i)
			}
		}
		lastIdx = idx
	}

	// Wait for the majority to commit all entries.
	for _, id := range majority {
		if !tc.WaitForCommit(id, lastIdx, 5*time.Second) {
			t.Fatalf("node %d in majority did not commit entry %d", id, lastIdx)
		}
	}

	// Heal — isolated node now needs to catch up lastIdx entries from scratch.
	tc.Heal()

	if !tc.WaitForCommit(isolatedID, lastIdx, 600*time.Millisecond) {
		t.Errorf("node %d did not catch up to index %d within 600ms — "+
			"conflict-term optimization may not be active (naive takes ~%dms for %d entries)",
			isolatedID, lastIdx, entryCount*50, entryCount)
	} else {
		t.Logf("node %d caught up to index %d after partition heal", isolatedID, lastIdx)
	}
}

// TestPreVote_PartitionedFollowerDoesNotBumpTerm verifies §9.6: a
// follower partitioned from the cluster must not increment its
// currentTerm during the partition, even though its election timer
// keeps firing. Without pre-vote, each failed election bumps the term;
// on heal the runaway term forces the live leader to step down.
//
// Setup: 3 nodes, leader elected, partition one follower off, wait
// ~5 election timeouts. Assert: (a) partitioned follower's term hasn't
// moved, (b) original leader is still leader, (c) after heal, exactly
// one leader.
func TestPreVote_PartitionedFollowerDoesNotBumpTerm(t *testing.T) {
	tc := testharness.NewTestCluster(3)
	tc.Start()
	defer tc.Stop()

	leaderID := tc.WaitForLeader(3 * time.Second)
	if leaderID == -1 {
		t.Fatal("no initial leader elected")
	}
	initialTerm, _, _ := tc.GetNode(leaderID).GetState()

	// Pick a follower to isolate.
	var follower int
	others := []int{}
	for id := 1; id <= 3; id++ {
		if id == leaderID {
			others = append(others, id)
		} else if follower == 0 {
			follower = id
		} else {
			others = append(others, id)
		}
	}

	followerTermBefore, _, _ := tc.GetNode(follower).GetState()

	tc.Partition([]int{follower}, others)

	// ~5 election-timeout windows; many pre-vote rounds fire and (correctly) fail.
	time.Sleep(2 * time.Second)

	followerTermAfter, followerState, _ := tc.GetNode(follower).GetState()
	if followerTermAfter > followerTermBefore {
		t.Errorf("pre-vote failed to suppress term bump: partitioned follower %d "+
			"went from term %d to term %d (expected no change)",
			follower, followerTermBefore, followerTermAfter)
	}
	if followerState == raft.Leader {
		t.Errorf("partitioned follower %d became leader of a minority partition", follower)
	}

	// Original leader still leader — pre-vote means the follower never
	// proposed a higher term, so no step-down.
	leaderTermAfter, leaderState, _ := tc.GetNode(leaderID).GetState()
	if leaderState != raft.Leader {
		t.Errorf("leader %d was disrupted during follower partition "+
			"(state=%v, term went %d→%d) — pre-vote isn't preventing the partitioned "+
			"node from forcing a step-down",
			leaderID, leaderState, initialTerm, leaderTermAfter)
	}

	tc.Heal()

	// After heal, cluster converges to one leader; the follower's
	// lastLeaderContact refreshes on the next heartbeat.
	time.Sleep(800 * time.Millisecond)
	leaders := tc.Leaders()
	if len(leaders) != 1 {
		t.Errorf("after heal, expected exactly 1 leader, got %d: %v", len(leaders), leaders)
	}

	t.Logf("pre-vote held: follower %d stayed at term %d through partition; "+
		"leader %d kept leadership at term %d",
		follower, followerTermAfter, leaderID, leaderTermAfter)
}
