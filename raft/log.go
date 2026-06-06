package raft

import "fmt"

// LogEntry represents a single entry in the replicated log.
// Each entry records the term when it was created and an arbitrary
// command (opaque bytes) that gets applied to the state machine
// once committed.
type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

// MemLog is an append-only sequence of entries that forms the core of
// Raft's replicated state. All log indices are 1-based to match the
// Raft paper's convention (section 5.3). Index 0 is unused.
type MemLog struct {
	entries []LogEntry
}

func NewMemLog() *MemLog {
	// Sentinel at index 0; real entries start at 1 (§5.3 indexing).
	return &MemLog{
		entries: []LogEntry{{Term: 0, Index: 0}},
	}
}

func (l *MemLog) LastIndex() int {
	return l.entries[len(l.entries)-1].Index
}

func (l *MemLog) LastTerm() int {
	return l.entries[len(l.entries)-1].Term
}

func (l *MemLog) Len() int {
	return len(l.entries) - 1
}

// Append adds a new entry and returns its index.
func (l *MemLog) Append(term int, command []byte) int {
	idx := l.LastIndex() + 1
	l.entries = append(l.entries, LogEntry{
		Term:    term,
		Index:   idx,
		Command: command,
	})
	return idx
}

func (l *MemLog) EntryAt(index int) (LogEntry, error) {
	if index < 0 || index >= len(l.entries) {
		return LogEntry{}, fmt.Errorf("index %d out of range [0, %d)", index, len(l.entries))
	}
	return l.entries[index], nil
}

func (l *MemLog) TermAt(index int) int {
	if index < 0 || index >= len(l.entries) {
		return -1
	}
	return l.entries[index].Term
}

// MatchCheck verifies that the log contains an entry at prevLogIndex with
// prevLogTerm. This is the consistency check from AppendEntries RPC
// (section 5.3): the leader includes the index and term of the entry
// immediately preceding the new ones, and the follower rejects if its
// log doesn't match. This is what guarantees the Log Matching Property.
func (l *MemLog) MatchCheck(prevLogIndex, prevLogTerm int) bool {
	if prevLogIndex == 0 {
		return true // matches sentinel
	}
	if prevLogIndex >= len(l.entries) {
		return false // follower log too short
	}
	return l.entries[prevLogIndex].Term == prevLogTerm
}

// AppendEntries handles incoming entries from a leader. It truncates
// any conflicting suffix (entries that differ in term from what the
// leader sent) and appends new entries. Returns the index of the last
// new entry.
//
// The truncation logic is critical for correctness: if a follower has
// stale entries from a previous leader, they must be overwritten.
// Section 5.3 of the paper specifies: "If an existing entry conflicts
// with a new one (same index but different terms), delete the existing
// entry and all that follow it."
func (l *MemLog) AppendEntries(prevLogIndex int, entries []LogEntry) int {
	insertAt := prevLogIndex + 1

	for i, entry := range entries {
		pos := insertAt + i
		if pos < len(l.entries) {
			if l.entries[pos].Term != entry.Term {
				// Conflict: truncate from here.
				l.entries = l.entries[:pos]
				l.entries = append(l.entries, entries[i:]...)
				return l.entries[len(l.entries)-1].Index
			}
			// Already have this entry with matching term, skip.
		} else {
			// Past the end, append remaining.
			l.entries = append(l.entries, entries[i:]...)
			return l.entries[len(l.entries)-1].Index
		}
	}

	return l.LastIndex()
}

// Slice returns entries from startIndex to the end (inclusive).
// Used by the leader to build AppendEntries payloads.
func (l *MemLog) Slice(startIndex int) []LogEntry {
	if startIndex >= len(l.entries) {
		return nil
	}
	if startIndex < 0 {
		startIndex = 0
	}
	// Return a copy to prevent mutation of the log.
	out := make([]LogEntry, len(l.entries)-startIndex)
	copy(out, l.entries[startIndex:])
	return out
}

// FirstIndexOfTerm returns the first log index whose term equals t, or -1.
func (l *MemLog) FirstIndexOfTerm(t int) int {
	for _, e := range l.entries[1:] {
		if e.Term == t {
			return e.Index
		}
	}
	return -1
}

// Entries is for testing and debugging only.
func (l *MemLog) Entries() []LogEntry {
	if len(l.entries) <= 1 {
		return nil
	}
	out := make([]LogEntry, len(l.entries)-1)
	copy(out, l.entries[1:])
	return out
}
