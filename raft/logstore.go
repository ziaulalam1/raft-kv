package raft

// LogStore is the replicated-log abstraction Node depends on.
// MemLog satisfies it in-memory; a fsync-per-append FileLog would
// slot in as a sibling without touching Node.
type LogStore interface {
	Append(term int, command []byte) int
	EntryAt(index int) (LogEntry, error)
	TermAt(index int) int
	MatchCheck(prevLogIndex, prevLogTerm int) bool
	AppendEntries(prevLogIndex int, entries []LogEntry) int
	Slice(startIndex int) []LogEntry
	LastIndex() int
	LastTerm() int
	FirstIndexOfTerm(t int) int
	Len() int
}

var _ LogStore = (*MemLog)(nil)
