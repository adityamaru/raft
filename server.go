package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

// Server is a wrapper which contains a `node` and an `rpc.Server`. It is
// responsible for managing RPC communications between `nodes`.
type Server struct {
	nodeID           int
	participantNodes []int

	node       *Node
	rpcMethods *RPCMethods
	rpcServer  *rpc.Server

	// This channel is used to indicate to the node that it has been connected
	// to all the other participants in the raft group.
	connectedToAllParticipants <-chan interface{}

	// Map from nodeID to that nodes `client` we can Call methods on.
	nodeIDToClient map[int]*rpc.Client

	listener net.Listener

	mu sync.Mutex

	// Wait group is used to synchronize all the RPCs which are sent to this server.
	waitGroup sync.WaitGroup
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

// StartServer is responsible for setting up the server side of the rpc server
// and accpeting future rpcs.
func (server *Server) StartServer() {
	server.mu.Lock()
	server.node = CreateNewNode(server.nodeID, server.participantNodes, server, server.connectedToAllParticipants)

	// Do the setup for the RPC server.
	server.rpcServer = rpc.NewServer()
	server.rpcMethods = &RPCMethods{server.node}
	server.rpcServer.RegisterName("Node", server.rpcMethods)

	var err error
	server.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Server for node %d is listening at %s", server.nodeID, server.listener.Addr())
	server.mu.Unlock()

	server.waitGroup.Add(1)
	go func() {
		defer server.waitGroup.Done()

		for {
			conn, err := server.listener.Accept()
			if err != nil {
				log.Fatal("Accept error encountered.")
			}
			server.waitGroup.Add(1)
			go func() {
				server.rpcServer.ServeConn(conn)
				server.waitGroup.Done()
			}()
		}
	}()
}

// ConnectToNode creates a `client` to communicate with the given nodeID.
func (server *Server) ConnectToNode(connectToID int, address net.Addr) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.nodeIDToClient[connectToID] == nil {
		client, err := rpc.Dial(address.Network(), address.String())
		if err != nil {
			return err
		}
		server.nodeIDToClient[connectToID] = client
	}
	return nil
}

// Call is used to invoke the RPC methods.
func (server *Server) Call(callOnID int, methodName string, args interface{}, reply interface{}) error {
	server.mu.Lock()
	client := server.nodeIDToClient[callOnID]
	server.mu.Unlock()

	if client == nil {
		return fmt.Errorf("Calling client id %d but it has either been disconnected or not created in the first place", callOnID)
	}
	return client.Call(methodName, args, reply)
}

// RPCMethods is a unique paradigm I saw online. It is a simple wrapper around the
// methods we want to register with the RPC server and helps us achieve the
// following:
//
// Avoid unneccessary logging due to https://github.com/golang/go/issues/19957
//
// It makes it easier to inject dropping of some RPCs for future testiing.
type RPCMethods struct {
	node *Node
}

// RequestVote is a wrapper.
func (rpcMethods *RPCMethods) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	return rpcMethods.node.RequestVote(args, reply)
}

// AppendEntries is a wrapper.
func (rpcMethods *RPCMethods) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	return rpcMethods.node.AppendEntries(args, reply)
}
