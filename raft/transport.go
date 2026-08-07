package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// --- RPC message types ---

type RequestVoteArgs struct {
	Term         int `json:"term"`
	CandidateId  int `json:"candidateId"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

// PreVoteArgs is the §9.6 pre-vote RPC: candidate asks peers if they
// would vote for it at proposedTerm without anyone advancing state.
// Filters out elections that can't win, so a node rejoining from a
// partition can't disrupt a live leader by proposing a runaway term.
type PreVoteArgs struct {
	Term         int `json:"term"`
	CandidateId  int `json:"candidateId"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

type PreVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderId     int        `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
	// Conflict info for the §5.3 optimization. Set by the follower on rejection
	// so the leader can skip back a full term instead of decrementing by one.
	// ConflictTerm = -1 means the follower's log is shorter than PrevLogIndex;
	// ConflictIndex is then the first index the follower is missing.
	ConflictTerm  int `json:"conflictTerm"`
	ConflictIndex int `json:"conflictIndex"`
}

// Transport is the interface for sending RPCs between nodes.
// The production implementation uses HTTP; tests use an in-process
// implementation that can simulate partitions and delays.
type Transport interface {
	SendRequestVote(peerID int, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(peerID int, args AppendEntriesArgs) (AppendEntriesReply, error)
	SendPreVote(peerID int, args PreVoteArgs) (PreVoteReply, error)
}

// --- HTTP Transport ---

// HTTPTransport sends RPCs over HTTP/JSON. Chose HTTP over gRPC
// because it requires zero external dependencies (net/http is stdlib).
// Binary encoding would be more efficient but adds no signal at this
// scale. The trade-off is acceptable for a 3-node demo cluster.
type HTTPTransport struct {
	peerAddrs map[int]string
	client    *http.Client
}

func NewHTTPTransport(peerAddrs map[int]string) *HTTPTransport {
	return &HTTPTransport{
		peerAddrs: peerAddrs,
		client: &http.Client{
			Timeout: rpcTimeout,
		},
	}
}

func (t *HTTPTransport) SendRequestVote(peerID int, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	addr, ok := t.peerAddrs[peerID]
	if !ok {
		return reply, fmt.Errorf("unknown peer %d", peerID)
	}

	body, _ := json.Marshal(args)
	resp, err := t.client.Post(addr+"/raft/vote", "application/json", bytes.NewReader(body))
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return reply, err
	}
	err = json.Unmarshal(data, &reply)
	return reply, err
}

func (t *HTTPTransport) SendPreVote(peerID int, args PreVoteArgs) (PreVoteReply, error) {
	var reply PreVoteReply
	addr, ok := t.peerAddrs[peerID]
	if !ok {
		return reply, fmt.Errorf("unknown peer %d", peerID)
	}

	body, _ := json.Marshal(args)
	resp, err := t.client.Post(addr+"/raft/prevote", "application/json", bytes.NewReader(body))
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return reply, err
	}
	err = json.Unmarshal(data, &reply)
	return reply, err
}

func (t *HTTPTransport) SendAppendEntries(peerID int, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	addr, ok := t.peerAddrs[peerID]
	if !ok {
		return reply, fmt.Errorf("unknown peer %d", peerID)
	}

	body, _ := json.Marshal(args)
	resp, err := t.client.Post(addr+"/raft/append", "application/json", bytes.NewReader(body))
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return reply, err
	}
	err = json.Unmarshal(data, &reply)
	return reply, err
}

// --- HTTP Handlers ---

// RegisterHandlers sets up HTTP endpoints for a Raft node. Called once
// during server startup.
func RegisterHandlers(mux *http.ServeMux, node *Node) {
	mux.HandleFunc("/raft/vote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var args RequestVoteArgs
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.HandleRequestVote(args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/prevote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var args PreVoteArgs
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.HandlePreVote(args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/append", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var args AppendEntriesArgs
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.HandleAppendEntries(args)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/status", func(w http.ResponseWriter, r *http.Request) {
		term, state, leader := node.GetState()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":          node.GetID(),
			"term":        term,
			"state":       state.String(),
			"leader":      leader,
			"commitIndex": node.GetCommitIndex(),
			"logLength":   node.GetLog().Len(),
		})
	})
}

// StartHTTPServer creates and starts an HTTP server for a node.
// Returns the server so it can be shut down later.
func StartHTTPServer(addr string, node *Node) *http.Server {
	mux := http.NewServeMux()
	RegisterHandlers(mux, node)

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go srv.ListenAndServe()
	return srv
}
