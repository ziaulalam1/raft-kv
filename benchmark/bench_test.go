package benchmark

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ziaulalam1/raft-kv/store"
	"github.com/ziaulalam1/raft-kv/testharness"
)

func TestMain(m *testing.M) {
	// Suppress raft logs during benchmarks for clean output.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// BenchmarkWriteThroughput measures how many write operations per second
// a 3-node cluster can sustain under serial submission. This is a lower
// bound: a real implementation would pipeline requests.
func BenchmarkWriteThroughput(b *testing.B) {
	tc := testharness.NewTestCluster(3)
	tc.Start()
	defer tc.Stop()

	leaderID := tc.WaitForLeader(3 * time.Second)
	if leaderID == -1 {
		b.Fatal("no leader elected")
	}

	// Warm up: let the cluster stabilize.
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := store.EncodeOp(store.Op{
			Type:  "put",
			Key:   fmt.Sprintf("bench-key-%d", i),
			Value: fmt.Sprintf("bench-val-%d", i),
		})
		_, _, ok := tc.GetNode(leaderID).Submit(op)
		if !ok {
			// Leader may have changed; find new leader.
			leaderID = tc.WaitForLeader(1 * time.Second)
			if leaderID == -1 {
				b.Fatal("lost leader during benchmark")
			}
			i-- // retry this iteration
		}
	}
}

// BenchmarkElectionConvergence measures how long it takes for a new
// leader to be elected after the current leader is killed. Run multiple
// trials to get stable statistics.
func BenchmarkElectionConvergence(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tc := testharness.NewTestCluster(3)
		tc.Start()

		leaderID := tc.WaitForLeader(3 * time.Second)
		if leaderID == -1 {
			tc.Stop()
			b.Fatal("no initial leader")
		}

		// Kill the leader and measure time to new leader.
		tc.KillNode(leaderID)
		start := time.Now()

		newLeader := tc.WaitForLeader(5 * time.Second)
		elapsed := time.Since(start)

		if newLeader == -1 {
			tc.Stop()
			b.Fatal("no new leader elected")
		}

		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/election")
		tc.Stop()
	}
}
