package raft

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
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

// CreateNewNode returns an instance of a node which will participate in the
// raft group.
func CreateNewNode(id int, participantNodes []int, server *Server, connectedToAllParticipants <-chan interface{}) *Node {
	node := new(Node)
	node.id = id
	node.participantNodes = participantNodes
	node.server = server
	node.state = follower
	node.votedFor = -1

	// The node must be connected to all of its peer before it can start its
	// election timer.
	go func() {
		<-connectedToAllParticipants
		node.mu.Lock()
		node.timeSinceTillLastReset = time.Now()
		node.mu.Unlock()

		node.startElectionTimer()
	}()

	return node
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

	log.Printf("RequestVote args: %+v\ncurrentTerm=%d\nvotedFor=%d", args, node.currentTerm, node.votedFor)

	// If the RPC term is less than the current term then we must reject the
	// vote request.
	if args.term < node.currentTerm {
		reply.term = node.currentTerm
		reply.voteGranted = false
		log.Printf("RequestVote has been rejected by %d", node.id)
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
	log.Printf("RequestVote reply: %+v", reply)
	return nil
}

// AppendEntriesArgs is the argument sent in an AppendEntries RPC.
type AppendEntriesArgs struct {
	term         int
	leaderID     int
	prevLogIndex int // index of log entry immediately preceding new ones.
	prevLogTerm  int // term of prevLogIndex entry.
	entries      []LogEntry
	leaderCommit int // leaders commitIndex.
}

// AppendEntriesReply is the respnse sent by an AppendEntries RPC.
type AppendEntriesReply struct {
	term    int
	success bool // true if the follower contained an entry matching prevLogIndex and prevLogTerm.
}

// AppendEntries is the RPC logic.
func (node *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.state == dead {
		return nil
	}

	log.Printf("AppendEntries args: %+v\ncurrentTerm=%d\n", args, node.currentTerm)
	// If the AppendEntries RPC is from a higher term then both followers and
	// candidates need to be reset.
	if args.term > node.currentTerm {
		node.updateStateToFollower(args.term)
	}

	if args.term == node.currentTerm {
		if node.state != follower {
			node.updateStateToFollower(args.term)
		}
		// Reset election timer since we have received a heartbeat from the leader.
		node.timeSinceTillLastReset = time.Now()

		// Compare prevLogIndex and prevLogTerm with our own log.
		if args.prevLogIndex == -1 || (args.prevLogIndex < len(node.log) && args.prevLogTerm == node.log[args.prevLogIndex].term) {
			reply.success = true

			// Find an existing entry that conflicts with the leader sent entries, and remove everything from it till the end.
			nodeLogIndex := args.prevLogIndex + 1
			leaderLogIndex := 0
			for {
				if nodeLogIndex >= len(node.log) {
					break
				}

				if leaderLogIndex >= len(args.entries) {
					break
				}

				// Found a mismatch so we need to overwrite from this index onwards.
				if args.entries[leaderLogIndex].term != node.log[nodeLogIndex].term {
					break
				}

				nodeLogIndex++
				leaderLogIndex++
			}

			// There are still some log entries which the leader needs to inform us about.
			if leaderLogIndex < len(args.entries) {
				log.Printf("The node %d has an old log %+v", node.id, node.log)
				node.log = append(node.log[:nodeLogIndex], args.entries[leaderLogIndex:]...)
				log.Printf("The node %d has a new log %+v", node.id, node.log)
			}

			if args.leaderCommit > node.commitIndex {
				node.commitIndex = intMin(args.leaderCommit, len(node.log)-1)
				log.Printf("The commit index node %d has been changed to %d", node.id, node.commitIndex)
				// Indicate to the client that this follower has committed new entries.
			}
		}

		reply.success = true
	}
	reply.term = node.currentTerm
	// By default but for readabilty.
	reply.success = false
	log.Printf("AppendEntries reply: %+v", reply)
	return nil
}

func intMin(a, b int) int {
	if a < b {
		return a
	}

	return b
}

// This method is responsible for resetting the nodes state to follower. The
// mutex mu must be held before invoking this method.
func (node *Node) updateStateToFollower(latestTerm int) {
	node.currentTerm = latestTerm
	node.state = follower
	node.votedFor = -1

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
		if node.state != candidate && node.state != follower {
			log.Printf("The node is in the %s state, no need to run election", node.state)
			node.mu.Unlock()
			return
		}

		// If the timer was started in a previous term then we can back off
		// because a newer go routine would have been spawned cooresponding to
		// the new term.
		if node.currentTerm != timerStartTerm {
			log.Printf("Election timer started in term %d but now node has latest term %d, so we can back off", timerStartTerm, node.currentTerm)
			return
		}

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
	currentTerm := node.currentTerm
	node.state = candidate
	node.votedFor = node.id
	node.timeSinceTillLastReset = time.Now()

	log.Printf("Node %d has become a candidate with currentTerm=%d", node.id, node.currentTerm)

	// We vote for ourselves.
	var votesReceived int32 = 1

	// Send votes to all the other machines in the raft group.
	for _, nodeID := range node.participantNodes {
		go func(id int) {
			voteRequestArgs := RequestVoteArgs{
				term:        currentTerm,
				candidateID: id,
			}

			var reply RequestVoteReply
			log.Printf("Sending a RequestVote to %d with args %+v", id, voteRequestArgs)

			if err := node.server.Call(id, "Node.RequestVote", voteRequestArgs, &reply); err == nil {
				log.Printf("Received a response for RequestVote from node %d saying %+v, for the election started by node %d", id, reply, node.id)

				node.mu.Lock()
				defer node.mu.Unlock()

				// If the state of the current node has changed by the time the election response arrives then we must back off.
				if node.state != candidate {
					log.Printf("The state of node %d has changed from candidate to %s while waiting for an election response", node.id, node.state)
					return
				}

				// If the node responds with a higher term then we must back off from the election.
				if reply.term > currentTerm {
					node.updateStateToFollower(reply.term)
					return
				}

				if reply.term == currentTerm {
					if reply.voteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						// Check for majority votes having been received.
						if votes > (len(node.participantNodes)+1)/2 {
							log.Printf("The election has been won by node %d", node.id)
							node.updateStateToLeader()
							return
						}
					}
				}
			}
		}(nodeID)
	}
}

func (node *Node) sendLeaderHeartbeats() {
	node.mu.Lock()
	currentTerm := node.currentTerm
	node.mu.Unlock()
	for _, nodeID := range node.participantNodes {
		go func(id int) {
			node.mu.Lock()
			nextLogIndex := node.nextIndex[id]
			prevLogIndex := nextLogIndex - 1
			var prevLogTerm int
			if prevLogIndex >= 0 {
				prevLogTerm = node.log[prevLogIndex].term
			}
			entries := node.log[nextLogIndex:]

			appendEntriesArg := AppendEntriesArgs{
				term:         currentTerm,
				leaderID:     node.id,
				prevLogIndex: prevLogIndex,
				prevLogTerm:  prevLogTerm,
				entries:      entries,
				leaderCommit: node.commitIndex,
			}
			node.mu.Unlock()

			var reply AppendEntriesReply
			log.Printf("Sending a AppendEntries to %d with args %+v", id, appendEntriesArg)

			if err := node.server.Call(id, "Node.AppendEntries", appendEntriesArg, &reply); err == nil {
				log.Printf("Received a response for AppendEntries from node %d saying %+v", id, reply)

				if reply.term > currentTerm {
					log.Printf("Leader %d is backing off cause it received a higher term reply", reply.term)
					node.mu.Lock()
					node.updateStateToFollower(reply.term)
					node.mu.Unlock()
					return
				}

				if node.state == leader && currentTerm == reply.term {
					if reply.success {
						node.nextIndex[id] = nextLogIndex + len(entries)
						node.matchIndex[id] = node.nextIndex[id] - 1
						log.Printf("Append entries reply from node %d. Succeeded nextIndex: %d matchIndex: %d", id, node.nextIndex[id], node.matchIndex[id])
					} else {
						node.nextIndex[id] = node.nextIndex[id] - 1
						log.Printf("Append entries reply from node %d. Failed nextIndex reduced to %d", id, node.nextIndex[id])
					}
				}
			}
		}(nodeID)
	}
}

// Assumes mutex mu is held when this method is called.
func (node *Node) updateStateToLeader() {
	node.state = leader
	log.Printf("Node %d has become the leader", node.id)

	go func() {

		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			node.sendLeaderHeartbeats()
			<-ticker.C
			node.mu.Lock()
			// The node is no longer the leader so we can back off from sending
			// heartbeats.
			if node.state != leader {
				node.mu.Unlock()
				return
			}
			node.mu.Unlock()
		}
	}()
}

// Submit is the method used by the `client` to submit commands to the Raft leader.
func (node *Node) Submit(command interface{}) bool {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.state == leader {
		logEntry := LogEntry{
			term:    node.currentTerm,
			Command: command,
		}
		node.log = append(node.log, logEntry)
		log.Printf("Leader %d has receieved a command %+v", node.id, logEntry)
		return true
	}
	return false
}
