package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ziaulalam1/raft-kv/raft"
	"github.com/ziaulalam1/raft-kv/store"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		runServer(os.Args[2:])
	case "put":
		runPut(os.Args[2:])
	case "get":
		runGet(os.Args[2:])
	case "status":
		runStatus(os.Args[2:])
	case "demo":
		runDemo()
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage:
  raft-kv serve  --id=N --port=PORT --peers=host:port,host:port,...
  raft-kv put    --addr=host:port KEY VALUE
  raft-kv get    --addr=host:port KEY
  raft-kv status --addr=host:port
  raft-kv demo   (starts a 3-node cluster on localhost)
`)
}

func runServer(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	id := fs.Int("id", 0, "Node ID (1-based)")
	port := fs.Int("port", 0, "HTTP port")
	peersStr := fs.String("peers", "", "Comma-separated peer addresses (id=host:port)")
	fs.Parse(args)

	if *id == 0 || *port == 0 {
		fmt.Fprintln(os.Stderr, "error: --id and --port are required")
		os.Exit(1)
	}

	// Parse peers: "2=localhost:8002,3=localhost:8003"
	peerAddrs := make(map[int]string)
	var peerIDs []int
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) != 2 {
				fmt.Fprintf(os.Stderr, "error: invalid peer format %q (want id=host:port)\n", p)
				os.Exit(1)
			}
			pid, _ := strconv.Atoi(parts[0])
			peerAddrs[pid] = "http://" + parts[1]
			peerIDs = append(peerIDs, pid)
		}
	}

	kv := store.NewKVStore()
	transport := raft.NewHTTPTransport(peerAddrs)
	node := raft.NewNode(*id, peerIDs, transport, func(entry raft.LogEntry) {
		kv.Apply(entry.Command)
	}, raft.NewMemLog())

	addr := fmt.Sprintf(":%d", *port)
	mux := http.NewServeMux()
	raft.RegisterHandlers(mux, node)

	// KV read endpoint (local, eventually consistent).
	mux.HandleFunc("/kv/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val, ok := kv.Get(key)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key":    key,
			"value":  val,
			"exists": ok,
		})
	})

	// KV write endpoint — submits the command through Raft.
	mux.HandleFunc("/kv/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		index, term, ok := node.Submit(body)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"index":    index,
			"term":     term,
			"accepted": ok,
			"leaderId": func() int { _, _, lid := node.GetState(); return lid }(),
		})
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go srv.ListenAndServe()
	node.Start()

	slog.Info("node listening", "id", *id, "addr", addr)

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("node shutting down", "id", *id)
	node.Stop()
	srv.Close()
}

func runPut(args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	addr := fs.String("addr", "localhost:8001", "Node address")
	fs.Parse(args)

	remaining := fs.Args()
	if len(remaining) < 2 {
		fmt.Fprintln(os.Stderr, "usage: raft-kv put --addr=host:port KEY VALUE")
		os.Exit(1)
	}

	op := store.EncodeOp(store.Op{Type: "put", Key: remaining[0], Value: remaining[1]})
	resp, err := http.Post("http://"+*addr+"/kv/submit", "application/json",
		strings.NewReader(string(op)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	if accepted, _ := result["accepted"].(bool); accepted {
		fmt.Printf("OK (index=%v, term=%v)\n", result["index"], result["term"])
	} else {
		fmt.Printf("REJECTED (leader=%v)\n", result["leaderId"])
	}
}

func runGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	addr := fs.String("addr", "localhost:8001", "Node address")
	fs.Parse(args)

	remaining := fs.Args()
	if len(remaining) < 1 {
		fmt.Fprintln(os.Stderr, "usage: raft-kv get --addr=host:port KEY")
		os.Exit(1)
	}

	resp, err := http.Get("http://" + *addr + "/kv/get?key=" + remaining[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	if exists, _ := result["exists"].(bool); exists {
		fmt.Printf("%s\n", result["value"])
	} else {
		fmt.Println("(not found)")
	}
}

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	addr := fs.String("addr", "localhost:8001", "Node address")
	fs.Parse(args)

	resp, err := http.Get("http://" + *addr + "/raft/status")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	fmt.Printf("Node %v: state=%v term=%v leader=%v commit=%v log=%v\n",
		result["id"], result["state"], result["term"],
		result["leader"], result["commitIndex"], result["logLength"])
}

func runDemo() {
	fmt.Println("Starting 3-node Raft cluster on localhost...")
	fmt.Println("  Node 1: http://localhost:8001")
	fmt.Println("  Node 2: http://localhost:8002")
	fmt.Println("  Node 3: http://localhost:8003")
	fmt.Println()

	stores := make(map[int]*store.KVStore)
	nodes := make(map[int]*raft.Node)
	servers := make(map[int]*http.Server)

	for id := 1; id <= 3; id++ {
		port := 8000 + id
		peerAddrs := make(map[int]string)
		var peerIDs []int
		for pid := 1; pid <= 3; pid++ {
			if pid != id {
				peerAddrs[pid] = fmt.Sprintf("http://localhost:%d", 8000+pid)
				peerIDs = append(peerIDs, pid)
			}
		}

		kv := store.NewKVStore()
		stores[id] = kv

		transport := raft.NewHTTPTransport(peerAddrs)
		node := raft.NewNode(id, peerIDs, transport, func(entry raft.LogEntry) {
			kv.Apply(entry.Command)
		}, raft.NewMemLog())
		nodes[id] = node

		mux := http.NewServeMux()
		raft.RegisterHandlers(mux, node)
		mux.HandleFunc("/kv/get", func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			val, ok := kv.Get(key)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"key": key, "value": val, "exists": ok,
			})
		})
		mux.HandleFunc("/kv/submit", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "POST only", http.StatusMethodNotAllowed)
				return
			}
			body, _ := io.ReadAll(r.Body)
			index, term, ok := node.Submit(body)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"index":    index,
				"term":     term,
				"accepted": ok,
				"leaderId": func() int { _, _, lid := node.GetState(); return lid }(),
			})
		})

		addr := fmt.Sprintf(":%d", port)
		srv := &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
		}
		servers[id] = srv
		go srv.ListenAndServe()
	}

	// Start all nodes.
	for _, node := range nodes {
		node.Start()
	}

	fmt.Println("Cluster started. Waiting for leader election...")
	time.Sleep(2 * time.Second)

	for id, node := range nodes {
		term, state, leader := node.GetState()
		fmt.Printf("  Node %d: state=%s term=%d leader=%d\n", id, state, term, leader)
	}

	fmt.Println()
	fmt.Println("Try:")
	fmt.Println("  raft-kv put --addr=localhost:8001 mykey myvalue")
	fmt.Println("  raft-kv get --addr=localhost:8002 mykey")
	fmt.Println("  raft-kv status --addr=localhost:8001")
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop.")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	for _, node := range nodes {
		node.Stop()
	}
	for _, srv := range servers {
		srv.Close()
	}
}
