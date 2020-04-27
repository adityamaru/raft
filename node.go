package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// LogEntry represents an entry in the nodes log.
type LogEntry struct {
	term    int
	Command interface{}
}

// NodeState is the enum type representing the different states of the node.
type nodeState int

const (
	follower nodeState = iota
	candidate
	leader
	dead
)

func (ns nodeState) String() string {
	switch ns {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	case dead:
		return "dead"
	default:
		panic("Undefined behavior in String method")
	}
}

// Node represents the entity which takes part in the raft consensus protocol.
type Node struct {
	id int

	// IDs of other nodes in the system.
	participantNodes []int

	state nodeState

	timeSinceTillLastReset time.Time

	server *Server

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  map[int]int
	matchIndex map[int]int

	mu sync.Mutex
}

// RequestVoteArgs are the arguments for the RequestVote RPC.
type RequestVoteArgs struct {
	term         int
	candidateID  int
	lastLogIndex int
	lastLogTerm  int
}

// RequestVoteReply is the response to the RequestVote RPC.
type RequestVoteReply struct {
	term        int
	voteGranted bool
}

// RequestVote is the RPC implementation.
func (node *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.state == dead {
		return nil
	}

	log.Print("RequestVote args: %+v\ncurrentTerm=%d\nvotedFor=%d", args, node.currentTerm, node.votedFor)

	// If the RPC term is less than the current term then we must reject the
	// vote request.
	if args.term < node.currentTerm {
		reply.term = node.currentTerm
		reply.voteGranted = false
		log.Print("RequestVote has been rejected by %d", node.id)
		return nil
	}

	if args.term > node.currentTerm {
		// Update the current node's state to follower.
		node.updateStateToFollower(args.term)
	}

	// If the above condition was not true then we have to ensure that we have
	// not voted for some other node with the same term.
	if args.term == node.currentTerm && (node.votedFor == -1 || node.votedFor == args.candidateID) {
		reply.voteGranted = true
		node.votedFor = args.candidateID
		node.timeSinceTillLastReset = time.Now()
	} else {
		reply.voteGranted = false
	}
	reply.term = node.currentTerm
	log.Print("RequestVote reply: %+v", reply)
	return nil
}

// This method is responsible for resetting the nodes state to follower. The
// mutex mu must be held before invoking this method.
func (node *Node) updateStateToFollower(latestTerm int) {
	node.currentTerm = latestTerm
	node.state = follower
	node.votedFor = -1

	// Reset and restart the election timer.
	node.timeSinceTillLastReset = time.Now()

	// Start the followers election timer concurrently.
	go node.startElectionTimer()
}

// Returns a pseudo random duration which is used as the electionTimeout for
// this node. The range of the duration is [150, 300] as specified in the paper.
func (node *Node) randElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// This method is resposnible for periodically checking if a new election is to
// be started by the node.
func (node *Node) startElectionTimer() {
	electionTimeout := node.randElectionTimeout()

	node.mu.Lock()
	timerStartTerm := node.currentTerm
	node.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	// This loops wakes every 10ms and checks if the conditions are conducive
	// for starting an election. This is not the most efficient and
	// theoretically we could just wake up every electionTimeout, but this
	// reduces testability/log readability.
	for {
		<-ticker.C

		node.mu.Lock()
		if node.state != candidate || node.state != follower {
			log.Print("The node is in the %s state, no need to run election", node.state)
			node.mu.Unlock()
			return
		}

		// Some logic about the timer having been started in a previous term.

		// Run an election if we have reached the election timeout.
		if timePassed := time.Since(node.timeSinceTillLastReset); timePassed > electionTimeout {
			node.runElection()
			node.mu.Unlock()
			return
		}

		node.mu.Unlock()
	}
}

// Converts a follower to a candidate and starts an election.
// Assumes mu is held when this method is invoked.
func (node *Node) runElection() {
	node.currentTerm++
	node.state = candidate
	node.votedFor = node.id
	node.timeSinceTillLastReset = time.Now()

	log.Print("Node %d has become a candidate with currentTerm=%d", node.id, node.currentTerm)

	votesReceived := 0

	// Sen
}
