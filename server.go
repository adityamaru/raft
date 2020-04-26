package raft

// Server is a wrapper which contains a `node` and an `rpc.Server`. It is responsible for managing RPC communications between `nodes`.
type Server struct {
	nodeID int
	participantNodes []int

}