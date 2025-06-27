package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type Command struct {
	Op    string
	Key   string
	Value string
}

type LogEntry struct {
	Term    int64
	Command Command
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Node struct {
	mu              sync.Mutex
	id              string
	peers           []string
	state           State
	currentTerm     int64
	votedFor        string
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	leaderID        string
	lastHeartbeat   time.Time
	electionTimeout time.Duration
	kvStore         map[string]string
	nextIndex       map[string]int
	matchIndex      map[string]int
	opCount         int       // For throughput
	lastReport      time.Time // For throughput
	lastRecovery    int64     // Last recovery time in ms
	totalLatency    int64     // Cumulative latency for averaging
	latencyCount    int       // Number of operations for averaging
	latencyFile     *os.File  // File for latency data
	throughputFile  *os.File  // File for throughput data
	recoveryFile    *os.File  // File for recovery data
}

func NewNode(id string, peers []string) *Node {
	latencyFile, err := os.OpenFile("latency.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open latency.csv: %v", err)
	}
	throughputFile, err := os.OpenFile("throughput.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open throughput.csv: %v", err)
	}
	recoveryFile, err := os.OpenFile("recovery.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open recovery.csv: %v", err)
	}

	// Write headers if files are new
	if info, _ := latencyFile.Stat(); info.Size() == 0 {
		fmt.Fprintln(latencyFile, "Timestamp,Latency_ms")
	}
	if info, _ := throughputFile.Stat(); info.Size() == 0 {
		fmt.Fprintln(throughputFile, "Timestamp,Throughput_ops_per_sec")
	}
	if info, _ := recoveryFile.Stat(); info.Size() == 0 {
		fmt.Fprintln(recoveryFile, "Timestamp,Recovery_ms")
	}

	node := &Node{
		id:              id,
		peers:           peers,
		state:           Follower,
		currentTerm:     0,
		votedFor:        "",
		log:             make([]LogEntry, 0),
		commitIndex:     -1,
		lastApplied:     -1,
		leaderID:        "",
		lastHeartbeat:   time.Now(),
		electionTimeout: time.Duration(1000+rand.Intn(1000)) * time.Millisecond,
		kvStore:         make(map[string]string),
		nextIndex:       make(map[string]int),
		matchIndex:      make(map[string]int),
		opCount:         0,
		lastReport:      time.Now(),
		lastRecovery:    0,
		totalLatency:    0,
		latencyCount:    0,
		latencyFile:     latencyFile,
		throughputFile:  throughputFile,
		recoveryFile:    recoveryFile,
	}
	log.Printf("NewNode: Initializing node %s with peers %v", id, peers)
	return node
}

func (n *Node) StartCLIServer(cliPort string) {
	listener, err := net.Listen("tcp", cliPort)
	if err != nil {
		log.Fatalf("Failed to start CLI server on %s: %v", cliPort, err)
	}
	defer listener.Close()
	log.Printf("CLI server started on %s", listener.Addr().String())
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("CLI accept error: %v", err)
			continue
		}
		go n.handleCLI(conn)
	}
}

func (n *Node) handleCLI(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		startTime := time.Now()
		cmd := scanner.Text()
		parts := strings.Fields(cmd)
		if len(parts) == 0 {
			fmt.Fprintf(conn, "Error: empty command\n")
			continue
		}
		n.mu.Lock()
		switch parts[0] {
		case "put":
			if len(parts) != 3 {
				fmt.Fprintf(conn, "Error: put requires key and value\n")
				n.mu.Unlock()
				continue
			}
			if n.state != Leader {
				fmt.Fprintf(conn, "Error: not the leader\n")
				n.mu.Unlock()
				continue
			}
			entry := LogEntry{
				Term:    n.currentTerm,
				Command: Command{Op: "put", Key: parts[1], Value: parts[2]},
			}
			n.log = append(n.log, entry)
			logIndex := len(n.log) - 1
			log.Printf("Node %s: Appended log entry %+v at index %d", n.id, entry, logIndex)
			n.mu.Unlock()
			committed := n.replicateLogAndCommit(logIndex)
			n.mu.Lock()
			if committed {
				if val, exists := n.kvStore[parts[1]]; exists && val == parts[2] {
					fmt.Fprintf(conn, "OK\n")
				} else {
					fmt.Fprintf(conn, "Error: key not applied correctly\n")
				}
			} else {
				fmt.Fprintf(conn, "Error: failed to commit\n")
			}
		case "get":
			if len(parts) != 2 {
				fmt.Fprintf(conn, "Error: get requires key\n")
				n.mu.Unlock()
				continue
			}
			value, exists := n.kvStore[parts[1]]
			if exists {
				fmt.Fprintf(conn, "%s\n", value)
			} else {
				fmt.Fprintf(conn, "Error: key not found\n")
			}
		case "list":
			for k, v := range n.kvStore {
				fmt.Fprintf(conn, "%s=%s\n", k, v)
			}
		case "nodes":
			fmt.Fprintf(conn, "Node %s: state=%d, term=%d, leader=%s\n", n.id, n.state, n.currentTerm, n.leaderID)
			for _, peer := range n.peers {
				fmt.Fprintf(conn, "Peer %s\n", peer)
			}
		case "latency":
			if n.latencyCount > 0 {
				avgLatency := float64(n.totalLatency) / float64(n.latencyCount)
				fmt.Fprintf(conn, "Average latency: %.2f ms\n", avgLatency)
			} else {
				fmt.Fprintf(conn, "No operations recorded yet\n")
			}
		case "throughput":
			if time.Since(n.lastReport) > time.Second {
				n.opCount = 0
				n.lastReport = time.Now()
			}
			fmt.Fprintf(conn, "Throughput: %d ops/s\n", n.opCount)
		case "recovery":
			fmt.Fprintf(conn, "Last recovery time: %d ms\n", n.lastRecovery)
		default:
			fmt.Fprintf(conn, "Error: unknown command\n")
		}
		latency := time.Since(startTime).Milliseconds()
		n.totalLatency += latency
		n.latencyCount++
		n.opCount++
		// Log latency to file
		fmt.Fprintf(n.latencyFile, "%d,%d\n", time.Now().UnixNano()/int64(time.Millisecond), latency)

		if time.Since(n.lastReport) >= time.Second {
			// Log throughput to file
			fmt.Fprintf(n.throughputFile, "%d,%d\n", time.Now().UnixNano()/int64(time.Millisecond), n.opCount)
			log.Printf("Node %s: Throughput: %d ops/s", n.id, n.opCount)
			n.opCount = 0
			n.lastReport = time.Now()
		}
		log.Printf("Node %s: Command %s completed in %d ms", n.id, cmd, latency)
		n.mu.Unlock()
	}
}

func (n *Node) waitForPeers() bool {
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			log.Printf("Node %s: Timeout waiting for peers", n.id)
			return false
		case <-ticker.C:
			alive := 0
			for _, peer := range n.peers {
				conn, err := net.DialTimeout("tcp", peer, 100*time.Millisecond)
				if err == nil {
					conn.Close()
					alive++
				}
			}
			if alive == len(n.peers) {
				log.Printf("Node %s: All peers are alive", n.id)
				return true
			}
			log.Printf("Node %s: Waiting for %d/%d peers", n.id, alive, len(n.peers))
		}
	}
}

func (n *Node) Run() {
	ready := make(chan struct{})
	go func() {
		listener, err := net.Listen("tcp", n.id)
		if err != nil {
			log.Fatalf("Failed to start RPC server on %s: %v", n.id, err)
		}
		defer listener.Close()
		log.Printf("RPC server started on %s", n.id)
		close(ready)
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("RPC accept error: %v", err)
				continue
			}
			go n.handleRPC(conn)
		}
	}()

	<-ready
	if n.waitForPeers() {
		n.StartElection()
	} else {
		log.Printf("Node %s: Proceeding with partial cluster", n.id)
	}
	go n.leaderHeartbeat()

	var lastState State
	for {
		n.mu.Lock()
		state := n.state
		timeSince := time.Since(n.lastHeartbeat)
		n.mu.Unlock()

		if state != lastState {
			log.Printf("Run: state=%d, timeSinceHeartbeat=%v, timeout=%v", state, timeSince, n.electionTimeout)
			lastState = state
		}

		switch state {
		case Follower, Candidate:
			if timeSince > n.electionTimeout {
				n.StartElection()
			}
		case Leader:
			// Leader relies on leaderHeartbeat
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (n *Node) StartElection() {
	n.mu.Lock()
	startTime := time.Now()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.lastHeartbeat = time.Now()
	log.Printf("Node %s starting election for term %d", n.id, n.currentTerm)
	votes := 1
	lastLogIndex := len(n.log) - 1
	lastLogTerm := int64(0)
	if lastLogIndex >= 0 {
		lastLogTerm = n.log[lastLogIndex].Term
	}
	term := n.currentTerm
	totalNodes := len(n.peers) + 1
	n.mu.Unlock()

	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			if n.requestVote(peer, &args, &reply) {
				mu.Lock()
				votes++
				log.Printf("Node %s received vote from %s for term %d", n.id, peer, term)
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if votes > totalNodes/2 && n.state == Candidate && n.currentTerm == term {
		n.state = Leader
		n.leaderID = n.id
		n.lastRecovery = time.Since(startTime).Milliseconds()
		// Log recovery to file
		fmt.Fprintf(n.recoveryFile, "%d,%d\n", time.Now().UnixNano()/int64(time.Millisecond), n.lastRecovery)
		log.Printf("Node %s became leader for term %d with %d/%d votes, recovery time: %d ms", n.id, n.currentTerm, votes, totalNodes, n.lastRecovery)
		for _, peer := range n.peers {
			n.nextIndex[peer] = len(n.log)
			n.matchIndex[peer] = -1
		}
	} else {
		log.Printf("Node %s failed to become leader for term %d with %d/%d votes", n.id, n.currentTerm, votes, totalNodes)
		n.state = Follower
		n.votedFor = ""
	}
}

func (n *Node) leaderHeartbeat() {
	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			log.Printf("Node %s: Leader heartbeat stopped, state=%d", n.id, n.state)
			return
		}
		n.mu.Unlock()
		n.replicateLog()
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) replicateLog() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	log.Printf("Node %s: Sending heartbeats for term %d, log length=%d", n.id, n.currentTerm, len(n.log))
	term := n.currentTerm
	commitIndex := n.commitIndex
	logEntries := make([]LogEntry, len(n.log))
	copy(logEntries, n.log)
	peerNext := make(map[string]int)
	for peer, idx := range n.nextIndex {
		peerNext[peer] = idx
	}
	n.mu.Unlock()

	var wg sync.WaitGroup
	successCount := 1 // Leader itself
	successMu := sync.Mutex{}
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			next := peerNext[peer]
			prevIndex := next - 1
			prevTerm := int64(0)
			if prevIndex >= 0 && prevIndex < len(logEntries) {
				prevTerm = logEntries[prevIndex].Term
			}
			entries := logEntries[next:]
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			reply := AppendEntriesReply{}
			success := n.appendEntries(peer, &args, &reply)
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.state != Leader || n.currentTerm != term {
				log.Printf("Leader %s: State changed during heartbeat to %s; aborting", n.id, peer)
				return
			}
			if success {
				successMu.Lock()
				successCount++
				successMu.Unlock()
				n.nextIndex[peer] = next + len(entries)
				n.matchIndex[peer] = n.nextIndex[peer] - 1
				log.Printf("Node %s: Replicated to %s, nextIndex=%d, matchIndex=%d", n.id, peer, n.nextIndex[peer], n.matchIndex[peer])
			} else if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
				n.leaderID = ""
				log.Printf("Node %s stepping down: higher term %d detected from %s", n.id, reply.Term, peer)
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()
	totalNodes := len(n.peers) + 1
	log.Printf("Node %s: Replication complete, successCount=%d, totalNodes=%d", n.id, successCount, totalNodes)
	if successCount > totalNodes/2 {
		newCommitIndex := len(n.log) - 1
		if newCommitIndex > n.commitIndex {
			n.commitIndex = newCommitIndex
			log.Printf("Node %s: Updated commitIndex to %d with successCount=%d/%d", n.id, n.commitIndex, successCount, totalNodes)
			n.mu.Unlock()
			n.applyCommittedEntries()
			n.mu.Lock()
		}
	}
	n.mu.Unlock()
}

func (n *Node) replicateLogAndCommit(logIndex int) bool {
	log.Printf("Node %s: Starting replicateLogAndCommit for logIndex=%d", n.id, logIndex)
	n.replicateLog()
	n.mu.Lock()
	defer n.mu.Unlock()
	committed := n.commitIndex >= logIndex
	log.Printf("Node %s: logIndex=%d, commitIndex=%d, committed=%t", n.id, logIndex, n.commitIndex, committed)
	if committed {
		n.mu.Unlock()
		n.applyCommittedEntries()
		n.mu.Lock()
	}
	return committed
}

func (n *Node) applyCommittedEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Printf("Node %s: Applying entries from lastApplied=%d to commitIndex=%d", n.id, n.lastApplied, n.commitIndex)
	for i := n.lastApplied + 1; i <= n.commitIndex && i < len(n.log); i++ {
		entry := n.log[i]
		if entry.Command.Op == "put" {
			n.kvStore[entry.Command.Key] = entry.Command.Value
			log.Printf("Node %s: Applied to kvStore: %s=%s at index=%d", n.id, entry.Command.Key, entry.Command.Value, i)
		}
		n.lastApplied = i
	}
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

type RequestVoteArgs struct {
	Term         int64
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int64
}

type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

func (n *Node) appendEntries(peer string, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	for i := 0; i < 3; i++ {
		conn, err := net.DialTimeout("tcp", peer, 100*time.Millisecond)
		if err != nil {
			log.Printf("Failed to connect to %s: %v, attempt %d", peer, err, i+1)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		defer conn.Close()
		err = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			log.Printf("Failed to set deadline on connection to %s: %v", peer, err)
			continue
		}
		entriesStr := strings.Join(encodeEntries(args.Entries), ",")
		if entriesStr == "" {
			entriesStr = "none"
		}
		rpcMsg := fmt.Sprintf("AppendEntries %d %s %d %d %d %s", args.Term, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, entriesStr)
		log.Printf("Node %s sending to %s: %s", n.id, peer, rpcMsg)
		_, err = fmt.Fprintf(conn, "%s\n", rpcMsg)
		if err != nil {
			log.Printf("Failed to send AppendEntries to %s: %v", peer, err)
			continue
		}
		reader := bufio.NewReader(conn)
		log.Printf("Node %s waiting for response from %s", n.id, peer)
		resp, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Failed to read response from %s: %v", peer, err)
			continue
		}
		resp = strings.TrimSpace(resp)
		log.Printf("Node %s received response from %s: %s", n.id, peer, resp)
		_, err = fmt.Sscanf(resp, "Reply %d %t", &reply.Term, &reply.Success)
		if err != nil {
			log.Printf("Failed to parse response from %s: '%s', error: %v", peer, resp, err)
			continue
		}
		log.Printf("Node %s sent heartbeat to %s: success=%t, replyTerm=%d", n.id, peer, reply.Success, reply.Term)
		return reply.Success
	}
	log.Printf("Node %s failed to send heartbeat to %s after 3 attempts", n.id, peer)
	return false
}

func (n *Node) handleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	log.Printf("Node %s: Entering handleAppendEntries from %s", n.id, args.LeaderID)
	n.mu.Lock()
	reply := AppendEntriesReply{Term: n.currentTerm}
	log.Printf("Node %s: Processing AppendEntries: term=%d, prevIndex=%d, entries=%v", n.id, args.Term, args.PrevLogIndex, args.Entries)
	if args.Term < n.currentTerm {
		reply.Success = false
		log.Printf("Node %s rejected AppendEntries from %s: term %d < %d", n.id, args.LeaderID, args.Term, n.currentTerm)
		n.mu.Unlock()
		return reply
	}
	n.lastHeartbeat = time.Now()
	n.state = Follower
	n.leaderID = args.LeaderID
	log.Printf("Node %s: Updated state to Follower, leader=%s", n.id, args.LeaderID)
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		log.Printf("Node %s: Updated term to %d", n.id, args.Term)
	}
	if args.PrevLogIndex >= len(n.log) || (args.PrevLogIndex >= 0 && n.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		log.Printf("Node %s rejected AppendEntries from %s: log mismatch at index %d", n.id, args.LeaderID, args.PrevLogIndex)
		n.mu.Unlock()
		return reply
	}
	n.log = append(n.log[:args.PrevLogIndex+1], args.Entries...)
	log.Printf("Node %s: Updated log to %+v", n.id, n.log)
	n.commitIndex = min(args.LeaderCommit, len(n.log)-1)
	log.Printf("Node %s: Updated commitIndex to %d", n.id, n.commitIndex)
	reply.Success = true
	log.Printf("Node %s successfully processed AppendEntries from %s, commitIndex=%d", n.id, args.LeaderID, n.commitIndex)
	n.mu.Unlock()
	n.applyCommittedEntries()
	return reply
}

func (n *Node) requestVote(peer string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	conn, err := net.DialTimeout("tcp", peer, 100*time.Millisecond)
	if err != nil {
		log.Printf("Failed to connect to %s: %v", peer, err)
		return false
	}
	defer conn.Close()
	err = conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
	if err != nil {
		log.Printf("Failed to set deadline on connection to %s: %v", peer, err)
		return false
	}
	rpcMsg := fmt.Sprintf("RequestVote %d %s %d %d", args.Term, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
	log.Printf("Node %s sending to %s: %s", n.id, peer, rpcMsg)
	_, err = fmt.Fprintf(conn, "%s\n", rpcMsg)
	if err != nil {
		log.Printf("Failed to send RequestVote to %s: %v", peer, err)
		return false
	}
	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Failed to read response from %s: %v", peer, err)
		return false
	}
	resp = strings.TrimSpace(resp)
	log.Printf("Node %s received response from %s: %s", n.id, peer, resp)
	_, err = fmt.Sscanf(resp, "Reply %d %t", &reply.Term, &reply.VoteGranted)
	if err != nil {
		log.Printf("Failed to parse response from %s: '%s', error: %v", peer, resp, err)
		return false
	}
	return reply.VoteGranted
}

func (n *Node) handleRPC(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	req, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("RPC read error on %s: %v", n.id, err)
		return
	}
	req = strings.TrimSpace(req)
	log.Printf("Node %s received RPC: %s", n.id, req)
	parts := strings.Fields(req)
	if len(parts) < 1 {
		log.Printf("Empty RPC request received on %s", n.id)
		return
	}
	switch parts[0] {
	case "AppendEntries":
		if len(parts) < 6 {
			log.Printf("Node %s: Malformed AppendEntries request: %s", n.id, req)
			return
		}
		var args AppendEntriesArgs
		entriesStr := "none"
		_, err := fmt.Sscanf(req, "AppendEntries %d %s %d %d %d", &args.Term, &args.LeaderID, &args.PrevLogIndex, &args.PrevLogTerm, &args.LeaderCommit)
		if err != nil {
			log.Printf("Node %s: Failed to parse AppendEntries: %v", n.id, err)
			return
		}
		if len(parts) > 6 {
			entriesStr = strings.Join(parts[6:], " ")
		}
		if entriesStr != "none" {
			args.Entries = decodeEntries(strings.Split(entriesStr, ","))
		}
		log.Printf("Node %s: Calling handleAppendEntries for %s", n.id, args.LeaderID)
		reply := n.handleAppendEntries(args)
		resp := fmt.Sprintf("Reply %d %t\n", reply.Term, reply.Success)
		log.Printf("Node %s: Sending reply to %s: %s", n.id, args.LeaderID, resp)
		_, err = conn.Write([]byte(resp))
		if err != nil {
			log.Printf("Node %s: Failed to send reply to %s: %v", n.id, args.LeaderID, err)
		} else {
			log.Printf("Node %s: Successfully sent reply to %s", n.id, args.LeaderID)
		}
	case "RequestVote":
		if len(parts) != 5 {
			log.Printf("Malformed RequestVote request: %s", req)
			return
		}
		var args RequestVoteArgs
		_, err := fmt.Sscanf(req, "RequestVote %d %s %d %d", &args.Term, &args.CandidateID, &args.LastLogIndex, &args.LastLogTerm)
		if err != nil {
			log.Printf("Failed to parse RequestVote request: %v, request: %s", err, req)
			return
		}
		reply := n.handleRequestVote(args)
		resp := fmt.Sprintf("Reply %d %t\n", reply.Term, reply.VoteGranted)
		log.Printf("Node %s: Sending reply to %s: %s", n.id, args.CandidateID, resp)
		_, err = conn.Write([]byte(resp))
		if err != nil {
			log.Printf("Failed to send RequestVote reply to %s: %v", args.CandidateID, err)
		}
	default:
		log.Printf("Unknown RPC command: %s", parts[0])
	}
}

func (n *Node) handleRequestVote(args RequestVoteArgs) RequestVoteReply {
	log.Printf("Node %s: Entering handleRequestVote from %s", n.id, args.CandidateID)
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := RequestVoteReply{Term: n.currentTerm}
	log.Printf("Node %s: Processing RequestVote: term=%d, lastLogIndex=%d, lastLogTerm=%d", n.id, args.Term, args.LastLogIndex, args.LastLogTerm)
	if args.Term < n.currentTerm {
		reply.VoteGranted = false
		log.Printf("Node %s rejected RequestVote from %s: term %d < %d", n.id, args.CandidateID, args.Term, n.currentTerm)
		return reply
	}
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		n.state = Follower
		n.lastHeartbeat = time.Now()
		log.Printf("Node %s: Stepped down to Follower due to higher term %d", n.id, args.Term)
	}
	lastLogIndex := len(n.log) - 1
	lastLogTerm := int64(0)
	if lastLogIndex >= 0 {
		lastLogTerm = n.log[lastLogIndex].Term
	}
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		n.votedFor = args.CandidateID
		n.lastHeartbeat = time.Now()
		reply.VoteGranted = true
		log.Printf("Node %s granted vote to %s for term %d", n.id, args.CandidateID, args.Term)
	} else {
		reply.VoteGranted = false
		log.Printf("Node %s denied vote to %s for term %d", n.id, args.CandidateID, args.Term)
	}
	return reply
}

func encodeEntries(entries []LogEntry) []string {
	var encoded []string
	for _, e := range entries {
		encoded = append(encoded, fmt.Sprintf("%d:%s:%s:%s", e.Term, e.Command.Op, e.Command.Key, e.Command.Value))
	}
	return encoded
}

func decodeEntries(encoded []string) []LogEntry {
	var entries []LogEntry
	for _, e := range encoded {
		if e == "" {
			continue
		}
		parts := strings.Split(e, ":")
		if len(parts) != 4 {
			log.Printf("Invalid entry format: %s", e)
			continue
		}
		term := int64(0)
		fmt.Sscanf(parts[0], "%d", &term)
		entries = append(entries, LogEntry{Term: term, Command: Command{Op: parts[1], Key: parts[2], Value: parts[3]}})
	}
	return entries
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: go run main.go <node-id> <cli-port> [peer-ids...]")
	}
	nodeID := os.Args[1]
	cliPort := os.Args[2]
	peers := os.Args[3:]

	// Ensure cliPort is valid (remove leading colon if present)
	if strings.HasPrefix(cliPort, ":") {
		cliPort = cliPort[1:]
	}

	node := NewNode(nodeID, peers)
	go node.Run()
	go node.StartCLIServer(":" + cliPort)

	log.Printf("Main: Node %s started with peers %v", nodeID, peers)
	select {}
}
