from __raft import database
import random
from __raft import raft_pb2
from __raft import raft_pb2_grpc
import grpc
import threading
import time
from concurrent import futures

class RaftNode(raft_pb2_grpc.RaftNodeServicesServicer, raft_pb2_grpc.RaftClientServiceServicer):
    def __init__(self, nodeId, db_path, node_address, cluster):
        self.node_address = node_address
        self.cluster_nodes = cluster
        self.db = database.DatabaseKV(db_path) # database object which stores the (key, value) pairs of data, in storage.

        self.meta_file = 'logs_node_' + str(nodeId) + '/metadata.txt' # saves nodeId, currentTerm, votedFor, commitLength
        # the following is stored on stable storage in disk
        with open(self.meta_file, 'r') as f:
            lines = f.read().split("\n")

        # if a new node is starting, then nothing to be found in lines
        if len(lines) <= 1:
            self.nodeId = nodeId
            self.currentTerm = 0
            self.votedFor = None
            self.commitLength = 0
            self.writeMetadata() # write the metadata to file
        else:
            # else fetch the previous state of these variables from the file
            self.nodeId = int(lines[0].split()[1])
            self.currentTerm = int(lines[1].split()[1])
            self.votedFor = None if lines[2].split()[1] == "None" else int(lines[2].split()[1])
            self.commitLength = int(lines[3].split()[1])
        
        
        self.log_file = 'logs_node_' + str(nodeId) + '/logs.txt' # log saved on file
        # retrieve the logs   
        with open(self.log_file) as f:
            lines = f.read().split('\n')
        #logs to be stored as: message term\n
        if (len(lines) == 1 and lines[0] == '') or len(lines) == 0:
            self.log = []
        else:
            self.log = lines[:-1]
        
        # the following can be in transient storage, can be lost in a crash with no issue
        self.state = 'follower'  # Possible states: follower, candidate, leader
        self.currentLeader = None
        self.votesReceived = {}
        self.sentLength = []
        self.ackedLength = []

        # assume that the leader has failed after 10 seconds if no heartbeat arrives, since the lease expires 
        # after 10s, and a heartbeat should have come in every 1 to 2 secs. Randomize it: to avoid having lots of 
        # nodes trying to become candidates at the same time.
        self.election_timeout =  self.generate_random_timeout()
        
        self.next_index = {}
        self.match_index = {}
        self.last_applied = -1
        self.leader_alive = False
        self.heartbeat_interval = 1 # seconds

        self.last_leader_communication_time = time.time()  # Timestamp of the last leader communication
        self.leader_check_interval = 5 # seconds
        # thread which monitors whether the leader is alive or not
        self.communication_monitor_thread = None
        self.heartbeat_thread = None

        self.dump_file = 'logs_node_' + str(nodeId) + '/dump.txt'

        # Start the grpc server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        raft_pb2_grpc.add_RaftNodeServicesServicer_to_server(self, self.server)
        raft_pb2_grpc.add_RaftClientServiceServicer_to_server(self, self.server)
        
        port = self.node_address.split(":")[1]
        self.server.add_insecure_port("[::]:" + port)
        self.server.start()
        print("Server started, listening on " + port)
        
        self.start_leader_communication_monitoring()


    def writeMetadata(self):
        with open(self.meta_file, 'w') as f:
            f.write("nodeId: " + str(self.nodeId) + "\n" + 
                    "currentTerm: " + str(self.currentTerm) + "\n" +
                    "votedFor: " + str(self.votedFor) + "\n" + 
                    "commitLength: " + str(self.commitLength) + "\n")


    # RPC for AppendEntries
    def AppendEntries(self, request, context):
        # know that the leader is alive
        self.last_leader_communication_time = time.time()
        self.leader_alive = True
        self.votedFor = None
        self.currentLeader = request.leaderId
        self.writeMetadata()
        
        response = raft_pb2.AppendEntriesReply()

        """if request.term < self.currentTerm:
            response.term = self.currentTerm
            response.success = False
            
            with open(self.dump_file, 'a') as f:
                f.write(f"Node {self.nodeId} rejected AppendEntries RPC from {request.leaderId}.\n")
                print(f"Node {self.nodeId} rejected AppendEntries RPC from {request.leaderId}.")
            
            return response"""


        # Step 2: Verify the log consistency.
        if request.prevLogIndex >= len(self.log) or (request.prevLogIndex > 0 and 
                                                    len(self.log) > request.prevLogIndex and self.log[request.prevLogIndex].split()[-1] != request.prevLogTerm):
            response.term = self.currentTerm
            response.success = False
            
            with open(self.dump_file, 'a') as f:
                f.write(f"Node {self.nodeId} rejected AppendEntries RPC from {request.leaderId}.\n")
                print(f"Node {self.nodeId} rejected AppendEntries RPC from {request.leaderId}.")
            
            return response


        with open(self.dump_file, 'a') as f:
            f.write(f"Node {self.nodeId} accepted AppendEntries RPC from {request.leaderId}.\n")
            print(f"Node {self.nodeId} accepted AppendEntries RPC from {request.leaderId}.")

        # Step 3: Append new entries to the log.
        self.log = self.log[:request.prevLogIndex + 1] + list(request.entries)

        # Step 4: Update commit index.
        if request.leaderCommit > self.commitLength:
            self.commitIndex = min(request.leaderCommit, len(self.log) - 1)

        response.term = self.currentTerm
        response.success = True
        return response


    def reset_election_timeout(self):
        """
        Reset the election timeout.
        """
        self.election_timeout = self.generate_random_timeout()

    
    def generate_random_timeout(self):
        """
        Generate a random election timeout between a certain range.
        """
        # Adjust these values based on your requirements
        min_timeout = 11000   # in milliseconds
        max_timeout = 20000   # in milliseconds
        return random.randint(min_timeout, max_timeout) / 1000  # Convert to seconds


    def RequestVote(self, request, context):
        """
        Implementation of RequestVote function.
        """
        
        candidate_id = request.candidateId
        last_log_index = request.lastLogIndex
        last_log_term = request.lastLogTerm

        vote_granted = False
        
        if self.state == "leader":
            #print("I am the leader")
            with open(self.dump_file, 'a') as f:
                f.write(f"Vote denied for Node {request.candidateId} in term {self.currentTerm}.\n")
                print(f"Vote denied for Node {request.candidateId} in term {self.currentTerm}.")

            return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=vote_granted)

        # If candidate's term is outdated, reject the vote request
        if request.term < self.currentTerm:
            with open(self.dump_file, 'a') as f:
                f.write(f"Vote denied for Node {request.candidateId} in term {self.currentTerm}.\n")
                print(f"Vote denied for Node {request.candidateId} in term {self.currentTerm}.")
            #print(f"Request term {request.term} is lesser than current term {self.currentTerm}")
            return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=vote_granted)
        
        # If this node has already voted for another candidate in this term, reject the vote request
        if self.votedFor is not None and self.votedFor != candidate_id:
            with open(self.dump_file, 'a') as f:
                f.write(f"Vote denied for Node {request.candidateId} in term {self.currentTerm}.\n")
                print(f"Vote denied for Node {request.candidateId} in term {self.currentTerm}.")
            #print(f"I have voted someone, voted", self.votedFor)
            return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=vote_granted)
        
        # Check if candidate's log is at least as up-to-date as this node's log
        if last_log_term < self.last_log_term() or \
            (last_log_term == self.last_log_term() and last_log_index < self.last_log_index()):
            #print("Those is false")
            with open(self.dump_file, 'a') as f:
                f.write(f"Vote denied for Node {request.candidateId} in term {self.currentTerm}.\n")
                print(f"Vote denied for Node {request.candidateId} in term {self.currentTerm}.")
            return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=vote_granted)
        
        # Grant the vote since candidate's log is up-to-date
        self.currentTerm = request.term
        self.votedFor = candidate_id
        vote_granted = True

        with open(self.dump_file, 'a') as f:
            f.write(f"Vote granted for Node {request.candidateId} in term {self.currentTerm}.\n")
            print(f"Vote granted for Node {request.candidateId} in term {self.currentTerm}.")

        return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=vote_granted)


    def start_heartbeat(self):
        """
        Start sending periodic heartbeats to followers.
        """
        # Start a background thread for sending heartbeats
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeats)
        self.heartbeat_thread.daemon = True  # Daemonize the thread
        self.heartbeat_thread.start()


    def last_log_term(self):
        return 0 if len(self.log) == 0 else int(self.log[-1].split()[-1])


    def last_log_index(self):
        return len(self.log) - 1


    def send_heartbeats(self):
        """
        Send periodic heartbeats to followers.
        """
        # reset the votedFor metadata
        self.votedFor = None
        self.writeMetadata()

        while self.state == "leader":
            # update this time for the leader itself
            
            with open(self.dump_file, 'a') as f:
                f.write(f"Leader {self.nodeId} sending heartbeat\n")
                print(f"Leader {self.nodeId} sending heartbeat")
            
            self.last_leader_communication_time = time.time()
            # Send AppendEntries RPCs with empty entries to followers
            self.send_append_entries_to_followers()
            
            # Sleep for the heartbeat interval
            time.sleep(self.heartbeat_interval)
        


    def send_append_entries_to_followers(self):
        """
        Send heartbeat messages to followers.
        """
        for follower_id in self.cluster_nodes:
            if follower_id != self.nodeId:
                self.send_append_entries(follower_id)



    def send_append_entries(self, follower_id):
        """
        Send AppendEntries RPC to a follower.
        """
        # Prepare AppendEntries request
        request = raft_pb2.AppendEntriesMsg(
            term=self.currentTerm,
            leaderId=self.nodeId,
            prevLogIndex= self.last_log_index(),
            prevLogTerm= self.last_log_term(),
            entries=[],  # No new entries for heartbeat
            leaderCommit=self.commitLength
        )

        # Establish gRPC channel to the follower
        channel = grpc.insecure_channel(self.cluster_nodes[follower_id])
        stub = raft_pb2_grpc.RaftNodeServicesStub(channel)
        try:
            # Send AppendEntries RPC
            response = stub.AppendEntries(request)
        except grpc.RpcError as e:
            with open(self.dump_file, 'a') as f:
                f.write(f"Error occurred while sending RPC to Node {follower_id}.\n")
                print(f"Error occurred while sending RPC to Node {follower_id}.")
        # Handle response if necessary
        #if response.term > self.currentTerm:
            # If follower's term is higher, step down as leader
        #    self.currentTerm = response.term
        #    self.step_down()



    def become_follower(self, term, leader_id):
        self.state = 'follower'
        self.currentTerm = term
        self.votedFor = None
        self.leaderId = leader_id
        self.writeMetadata()



    def become_candidate(self):
        """
        Transition to the candidate state.
        """
        # Increment current term
        self.currentTerm += 1
        
        # Vote for self
        self.votedFor = self.nodeId
        
        self.writeMetadata()
        
        # Reset election timeout
        self.reset_election_timeout()
        
        # Start a new election
        self.start_election()



    def start_election(self):
        """
        Start a new election by sending RequestVote RPCs to other nodes.
        """

        # Prepare RequestVote request
        request = raft_pb2.RequestVoteMsg(
            term = self.currentTerm,
            candidateId = self.nodeId,
            lastLogIndex = self.last_log_index(),
            lastLogTerm = self.last_log_term()
        )

        # Variables to track votes received
        votes_received = 1  # Vote for self
        votes_needed = (len(self.cluster_nodes) + 1) // 2  # Majority of votes needed

        # Send RequestVote RPC to other nodes
        for node_id, node_address in self.cluster_nodes.items():
            if node_id != self.nodeId:
                #print("sending")
                response = self.send_request_vote(node_address, request)
                
                if response != None and response.voteGranted:
                    votes_received += 1
        print(votes_needed, votes_received)
        # Check if received a majority of votes
        if votes_received >= votes_needed:
            self.become_leader()
        else:
            # Not enough votes received, start a new election timer
            self.reset_election_timeout()



    def send_request_vote(self, node_address, request):
        """
        Send RequestVote RPC to a node.
        """
        # Establish gRPC channel to the target node
        channel = grpc.insecure_channel(node_address)
        try:
            stub = raft_pb2_grpc.RaftNodeServicesStub(channel)
            response = stub.RequestVote(request)
        except grpc.RpcError as e:
            response = None
        # Send RequestVote RPC
        
        return response



    def become_leader(self):
        """
        Transition to the leader state.
        """
        # Set node state to leader
        self.leader_alive = True
        self.state = "leader"
        
        with open(self.dump_file, 'a') as f:
            f.write(f"Node {self.nodeId} became the leader for term {self.currentTerm}.\n")
            print(f"Node {self.nodeId} became the leader for term {self.currentTerm}.")
        
        # Initialize next_index and match_index for each follower
        self.next_index = {follower_id: self.last_log_index() + 1 for follower_id in self.cluster_nodes}
        self.match_index = {follower_id: 0 for follower_id in self.cluster_nodes}
        
        # Start sending heartbeat messages
        self.start_heartbeat()



    def step_down(self):
        """
        Step down as leader.
        """
        with open(self.dump_file, 'a') as f:
            f.write(f"{self.nodeId} Stepping down\n")
            print(f"{self.nodeId} Stepping down")
        
        # Set node state to follower 
        self.leader_alive = False
        self.state = "follower"
       
        # Clear next_index and match_index
        self.next_index = {}
        self.match_index = {}
        
        # Reset election timeout
        self.reset_election_timeout()



    def ServeClient(self, request, context):
        """
        Handle client requests.
        """
        if self.state != "leader":
            return raft_pb2.ServeClientReply(data="Fail: Not a leader", leaderId=self.currentLeader, success=False)
        else:
            
            with open(self.dump_file, 'a') as f:
                f.write(f"Node {self.nodeId} (leader) received an {request.Request} request.\n")
                print(f"Node {self.nodeId} (leader) received an {request.Request} request.\n")

            # If leader, process the request
            if request.Request.split()[0] == "GET":
                key = request.Request.split()[1]
                return raft_pb2.ServeClientReply(data=self.db.get(key), success=True)
            
            # Process the SET type request.
            entry = request.Request + " " + str(self.currentTerm) 
            self.log.append(entry)

            # save the entry to file
            with open(self.log_file, 'a') as f:
                f.write(entry + "\n")
            
            self.replicate_log_entry(entry)

            return raft_pb2.ServeClientReply(success=True)



    def replicate_log_entry(self, log_entry):
        """
        Replicate a log entry to other nodes in the cluster.
        """
        if self.state != "leader":
            return  # Only leaders replicate log entries

        # Prepare AppendEntries request
        append_entries_request = raft_pb2.AppendEntriesMsg(
            term=int(log_entry.split()[-1]),
            leaderId=self.nodeId,
            prevLogIndex=self.last_log_index(),
            prevLogTerm=self.last_log_term(),
            entries=[log_entry],
            leaderCommit=self.commitLength
        )

        # Send AppendEntries RPC to followers
        for follower_id, follower_address in self.cluster_nodes.items():
            if follower_id != self.nodeId:
                try:
                    channel = grpc.insecure_channel(follower_address)
                    stub = raft_pb2_grpc.RaftNodeServicesStub(channel)
                    response = stub.AppendEntries(append_entries_request)
                    self.process_append_entries_response(response)
                except grpc.RpcError as e:
                    with open(self.dump_file, 'a') as f:
                        f.write(f"Error occurred while sending RPC to Node {follower_id}.\n")
                        print(f"Error occurred while sending RPC to Node {follower_id}.")



    def process_append_entries_response(self, response):
        """
        Process response of AppendEntries RPC.
        """
        if response.success:
            # Update last leader communication time upon successful AppendEntries RPC
            self.last_leader_communication_time = time.time()
            self.leader_alive = True
            # If the AppendEntries RPC was successful, update commit_index
            if response.last_log_index > self.commitLength:
                self.commitLength = min(response.last_log_index, len(self.log) - 1)
                # Check if the entry is committed by a majority quorum
                if self.is_majority_committed(self.commitLength):
                    # Apply committed log entries to the database
                    self.apply_committed_entries_to_database()



    def is_majority_committed(self, index):
        """
        Check if a log entry at the given index is committed by a majority quorum.
        """
        # Calculate the number of nodes required for a majority quorum
        majority_count = (len(self.cluster_nodes) // 2) + 1
        # Check if the count of nodes that have replicated the log entry is greater than or equal to the majority count
        committed_count = sum(1 for node_id, match_index in self.match_index.items() if match_index >= index)
        return committed_count >= majority_count



    def apply_committed_entries_to_database(self):
        """
        Apply committed log entries to the database.
        """
        for index in range(self.last_applied + 1, self.commitLength + 1):
            log_entry = self.log[index]
            # Apply the command from the log entry to the database
            self.apply_command_to_database(log_entry)
            # Update last_applied index
            self.last_applied = index
            
            if self.state == "follower":
                with open(self.dump_file, 'a') as f:
                    f.write(f"Node {self.nodeId} (follower) committed the entry {log_entry} to the state machine.\n")
                    print(f"Node {self.nodeId} (follower) committed the entry {log_entry} to the state machine.")    
            
            if self.state == "leader":
                with open(self.dump_file, 'a') as f:
                    f.write(f"Node {self.nodeId} (leader) committed the entry {log_entry} to the state machine.\n")
                    print(f"Node {self.nodeId} (leader) committed the entry {log_entry} to the state machine.")  


    def apply_command_to_database(self, command):
        """
        Apply a command to the database.
        """
        # Logic to apply the command to the database
        cmd, key, value, term = command.split(" ")
        if cmd == "SET":
            self.db.store(key, value)



    def start_leader_communication_monitoring(self):
        """
        Start monitoring leader communication.
        """
        self.communication_monitor_thread = threading.Thread(target=self.monitor_leader_communication)
        self.communication_monitor_thread.daemon = True
        self.communication_monitor_thread.start()



    def monitor_leader_communication(self):
        """
        Monitor leader communication to detect leader failure.
        """
        while True:
            if self.state == "leader":
                current_time = time.time()
                #print("Times ", current_time-self.last_leader_communication_time, self.election_timeout)
                time.sleep(self.leader_check_interval)
                continue
            current_time = time.time()
            #print("Times ", current_time-self.last_leader_communication_time, self.election_timeout, " node ", self.nodeId, "state", self.state)
            if current_time - self.last_leader_communication_time > self.election_timeout:
                # if this condition is satisfied, assume that leader has failed, and start a new election.
                               
                # Save to dump file that a new election is being started
                with open(self.dump_file, 'a') as f:
                    f.write(f"Node {self.nodeId} election timer timed out, Starting election.\n")
                    print(f"Node {self.nodeId} election timer timed out, Starting election.")

                self.leader_alive = False
                self.become_candidate()
                
            time.sleep(self.leader_check_interval)



    def join(self):
        if self.communication_monitor_thread:
            self.communication_monitor_thread.join()
        if self.heartbeat_thread:
            self.heartbeat_thread.join()
        
        self.server.wait_for_termination()
