package raft

import (
	"net/rpc"
	"sync"
)

// Server is a wrapper which contains a `node` and an `rpc.Server`. It is
// responsible for managing RPC communications between `nodes`.
type Server struct {
	nodeID           int
	participantNodes []int

	node      *Node
	rpcServer *rpc.Server

	// This channel is used to indicate to the node that it has been connected
	// to all the other participants in the raft group.
	connectedToAllParticipants <-chan interface{}

	// Map from nodeID to that nodes `client` we can Call methods on.
	nodeIDToClient map[int]*rpc.Client

	mu sync.Mutex
}

// CreateNewServer returns a server with the provided nodeID and other fields
// initialized.
func CreateNewServer(nodeID int, participantNodes []int, connectedToAllParticipants <-chan interface{}) *Server {
	server := new(Server)
	server.nodeID = nodeID
	server.participantNodes = participantNodes
	server.connectedToAllParticipants = connectedToAllParticipants
	server.nodeIDToClient = make(map[int]*rpc.Client)

	return server
}

// StartServer is responsible for
func (server *Server) StartServer() {

}
