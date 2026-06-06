package store

import (
	"encoding/json"
	"sync"
)

// Op represents a KV operation encoded in a Raft log entry.
// The command bytes in each LogEntry are JSON-encoded Op structs.
type Op struct {
	Type  string `json:"type"`  // "put" or "delete"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// EncodeOp serializes an Op for storage in a Raft log entry.
func EncodeOp(op Op) []byte {
	data, _ := json.Marshal(op)
	return data
}

// DecodeOp deserializes an Op from a Raft log entry.
func DecodeOp(data []byte) (Op, error) {
	var op Op
	err := json.Unmarshal(data, &op)
	return op, err
}

// KVStore is a simple in-memory key-value store that applies operations
// from committed Raft log entries. It sits on top of the Raft consensus
// layer: Raft handles replication and ordering, the KV store handles
// state.
//
// This separation matters because Raft itself is a replicated log, not
// a database. The log entries are opaque bytes to Raft. The KV store
// gives those bytes meaning by interpreting them as put/delete operations.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Apply processes a committed log entry. Called by the Raft node's
// applyFunc callback when an entry is committed.
func (s *KVStore) Apply(command []byte) {
	op, err := DecodeOp(command)
	if err != nil {
		return // skip malformed entries
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch op.Type {
	case "put":
		s.data[op.Key] = op.Value
	case "delete":
		delete(s.data, op.Key)
	}
}

// Get retrieves a value by key. Returns the value and whether the key
// exists. This reads local state, which is eventually consistent: the
// node may not have applied the latest committed entries yet.
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

// Snapshot returns a copy of all key-value pairs. Used for debugging
// and testing only.
func (s *KVStore) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
