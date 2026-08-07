# raft-kv

[![CI](https://github.com/ziaulalam1/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/ziaulalam1/raft-kv/actions/workflows/ci.yml)

Distributed key-value store built on Raft consensus. Zero external dependencies — leader election, log replication, and KV state machine are all stdlib Go.

The paper is clear about what to do. The implementation has dozens of places where one wrong detail (forgetting to reset `votedFor`, off-by-one in log indexing, stale term on RPC responses) breaks a safety guarantee that no behavior test will catch. That's why the test suite is built around invariants, not happy-path scenarios.

## What it does

A 3-node cluster that replicates key-value operations through Raft consensus. Any node can receive a write, but only the leader appends to the log and replicates to followers. Once a majority acknowledges, the entry is committed and applied to the KV state machine.

```bash
# Start a 3-node demo cluster
make demo

# In another terminal:
./raft-kv put --addr=localhost:8001 mykey myvalue
./raft-kv get --addr=localhost:8002 mykey    # reads from a follower
./raft-kv status --addr=localhost:8001       # shows term, state, leader
```

## Benchmark results

3-node in-process cluster, Go 1.26.2, arm64 Linux:

```
Write throughput:       ~87,000 ops/sec (11-12 us/op, serial submission)
Election convergence:   417ms avg (363-451ms range over 22 trials)
```

Serial submission only (one write at a time, wait for accept). The 300-500ms election window is intentionally wide to reduce split-vote probability.

## Invariant tests

Five tests verify the core Raft safety properties. All pass with the Go race detector enabled (`go test -race`).

| Test | Property | What it does |
|------|----------|-------------|
| `TestElectionSafety_AtMostOneLeaderPerTerm` | Election Safety | Runs 20 election rounds (kill leader, wait for re-election), verifies no term has two leaders |
| `TestLogMatching_SameIndexTermImpliesPrefixMatch` | Log Matching | Submits 5 entries, compares logs pairwise across all nodes, verifies prefix consistency |
| `TestLeaderCompleteness_CommittedEntryInFutureLeaders` | Leader Completeness | Commits an entry, kills the leader, verifies the new leader has the committed entry |
| `TestPartitionSafety_MinorityCannotElect` | Quorum | Partitions a 5-node cluster into 2+3, verifies the minority side cannot elect a leader |
| `TestConvergence_LogsMatchAfterPartitionHeal` | Convergence | Partitions, writes to majority, heals, verifies all nodes converge to the same log |

```bash
make race    # runs all tests with -race
make bench   # throughput and election convergence
```

## Architecture decisions

| Decision | Why |
|----------|-----|
| HTTP/JSON for inter-node RPC | Zero external dependencies. `net/http` is stdlib — no protoc, no codegen, no build step. Binary encoding would be faster; the tradeoff is acceptable for a 3-node correctness demo. |
| In-process cluster for tests | Deterministic message interception: inject partitions, drop messages, control timing without port flakiness. Test transport and HTTP transport implement the same `Transport` interface, so the node code is identical in both modes. |
| No persistent WAL | `fsync` ordering, crash recovery, and CRC checksums are a separate project. A restarted node loses its log and catches up from the leader. Adding a WAL here would add code without adding consensus signal. |
| No log compaction | Leader election and log replication are the core of Raft. Compaction (§7) is an optimization for long-running clusters; unbounded log growth is fine for a demo. |
| Go stdlib only | No `hashicorp/raft`, no `etcd/raft`. Every line is written from the paper, not from calling a library. |
| Race detector in CI | `go test -race` instruments memory accesses at compile time. Passing it means the concurrent code is actually correct, not just passing today's test runs. |

## What I would change

1. **Persistent WAL.** Each node would write log entries to an append-only file with `fsync` after each write and CRC32 checksums per entry. On restart, replay the WAL to recover state. This is the most important missing piece for production use.

2. **Snapshot transfer.** After log compaction, a lagging or new node cannot catch up entry-by-entry. The leader needs an `InstallSnapshot` RPC that transfers the entire state machine snapshot. Raft paper section 7.

3. **Read leases.** Currently, reads go through the leader (which adds a network hop). A read lease gives the leader a time-bounded guarantee of continued leadership, allowing it to serve reads locally. Followers could also serve stale reads with bounded staleness.

4. **Batching and pipelining.** AppendEntries currently sends one RPC per heartbeat interval. Batching multiple entries per RPC and pipelining (send the next batch before the current one is acknowledged) would improve throughput by 5-10x.

## Where this pattern appears

Raft is not specific to key-value stores. Replicate a log of operations so all nodes apply them in the same order, and you have a building block for any system that needs consistency across machines:

- **Databases**: CockroachDB and TiDB use Raft for per-range replication
- **Configuration stores**: etcd (the control plane for Kubernetes), Consul, ZooKeeper (uses ZAB, a Paxos variant)
- **Messaging**: Kafka KRaft mode replaces ZooKeeper with a Raft-based metadata quorum
- **Financial systems**: total-order broadcast for trade settlement, order matching with deterministic replay

## Project structure

```
raft-kv/
  main.go               -- CLI: serve, put, get, status, demo
  raft/
    node.go              -- Raft state machine, election, replication
    log.go               -- Replicated log with consistency checks
    transport.go         -- HTTP transport and RPC message types
    raft_test.go         -- 7 invariant tests
  store/
    kv.go                -- KV state machine applied from committed log
    kv_test.go           -- KV correctness tests (put, get, delete, partition)
  testharness/
    cluster.go           -- In-process test cluster with message interception
    partition.go         -- Network partition injection
  benchmark/
    bench_test.go        -- Throughput and convergence benchmarks
  Makefile               -- build, test, race, bench, demo
```
