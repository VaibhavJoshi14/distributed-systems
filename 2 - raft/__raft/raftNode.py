from __raft import database
import random
import raft_pb2
import raft_pb2_grpc
import grpc

class RaftNode(raft_pb2_grpc.RaftNodeServicesServicer, raft_pb2_grpc.RaftClientServiceServicer):
    def __init__(self, nodeId, db, node_address):
        self.node_address = node_address
        self.cluster_nodes = [] # to fill.
        self.db = db # database object which stores the (key, value) pairs of data, in storage.

        
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
        self.currentRole = 'follower'  # Possible states: follower, candidate, leader
        self.currentLeader = None
        self.votesReceived = {}
        self.sentLength = []
        self.ackedLength = []

        # assume that the leader has failed after 10 seconds if no heartbeat arrives, since the lease expires 
        # after 10s, and a heartbeat should have come in every 1 to 2 secs. Randomize it: to avoid having lots of 
        # nodes trying to become candidates at the same time.
        self.timeoutLeaderFailed = random.randint(10, 15)
        self.election_timeout =  self.generate_random_timeout()
        
        self.next_index = {}
        self.match_index = {}
        self.last_applied = -1
        self.leader_alive = False
        self.heartbeat_interval = 1  # 1 seconds

    def writeMetadata(self):
        with open(self.meta_file, 'w') as f:
            f.write("nodeId: " + str(self.nodeId) + "\n" + 
                    "currentTerm: " + str(self.currentTerm) + "\n" +
                    "votedFor: " + str(self.votedFor) + "\n" + 
                    "commitLength: " + str(self.commitLength) + "\n")


    # RPC for AppendEntries
    def AppendEntries(self, request, context):
        response = raft_pb2.AppendEntriesReply()

        if request.term < self.currentTerm:
            response.term = self.currentTerm
            response.success = False
            return response

        # Step 1: Reset election timeout since the node received communication from a valid leader.
        self.reset_election_timeout()

        # Step 2: Verify the log consistency.
        if request.prevLogIndex >= len(self.log) or self.log[request.prevLogIndex].term != request.prevLogTerm:
            response.term = self.current_term
            response.success = False
            return response

        # Step 3: Append new entries to the log.
        self.log = self.log[:request.prevLogIndex + 1] + request.entries

        # Step 4: Update commit index.
        if request.leaderCommit > self.commitLength:
            self.commitIndex = min(request.leaderCommit, len(self.raft_node.log) - 1)

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
        min_timeout = 1500   # in milliseconds
        max_timeout = 2000   # in milliseconds
        return random.randint(min_timeout, max_timeout) / 1000  # Convert to seconds


    def RequestVote(self, request, context):
        """
        Implementation of RequestVote function.
        """
        candidate_id = request.candidateId
        last_log_index = request.lastLogIndex
        last_log_term = request.lastLogTerm

        vote_granted = False
        
        # If candidate's term is outdated, reject the vote request
        if term < self.current_term:
            return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=vote_granted)
        
        # If this node has already voted for another candidate in this term, reject the vote request
        if self.voted_for is not None and self.voted_for != candidate_id:
            return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=vote_granted)
        
        # Check if candidate's log is at least as up-to-date as this node's log
        if last_log_term < self.lastLogTerm or \
            (last_log_term == self.lastLogTerm and last_log_index < self.lastLogIndex):
            return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=vote_granted)
        
        # Grant the vote since candidate's log is up-to-date
        self.current_term = term
        self.voted_for = candidate_id
        vote_granted = True

        return raft_pb2.RequestVoteReply(term=self.currentTerm, vote_granted=vote_granted)


    def start_heartbeat(self):
        """
        Start sending periodic heartbeats to followers.
        """
        # Start a background thread for sending heartbeats
        heartbeat_thread = threading.Thread(target=self.send_heartbeats)
        heartbeat_thread.daemon = True  # Daemonize the thread
        heartbeat_thread.start()


    def last_log_term(self):
        return None if len(self.log) == 0 else int(self.log[-1].split()[-1])


    def last_log_index(self):
        return len(self.log) - 1


    def send_heartbeats(self):
        """
        Send periodic heartbeats to followers.
        """
        self.leader_alive = True

        while self.leader_alive:
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
        request = raft_pb2.AppendEntriesRequest(
            term=self.currentTerm,
            leader_id=self.nodeId,
            prev_log_index= self.last_log_index(),
            prev_log_term= self.last_log_term(),
            entries=[],  # No new entries for heartbeat
            leader_commit=self.commit_index
        )

        # Establish gRPC channel to the follower
        channel = grpc.insecure_channel(self.cluster_nodes[follower_id])
        stub = raft_pb2_grpc.RaftServiceStub(channel)

        # Send AppendEntries RPC
        response = stub.AppendEntries(request)

        # Handle response if necessary
        if response.term > self.current_term:
            # If follower's term is higher, step down as leader
            self.current_term = response.term
            self.step_down()



    def become_follower(self, term, leader_id):
        self.state = 'follower'
        self.currentTerm = term
        self.votedFor = None
        self.leaderId = leader_id

    
    def become_candidate(self):
        """
        Transition to the candidate state.
        """
        # Increment current term
        self.currentTerm += 1
        
        # Vote for self
        self.votedFor = self.nodeId
        
        # Reset election timeout
        self.reset_election_timeout()
        
        # Start a new election
        self.start_election()


    def start_election(self):
        """
        Start a new election by sending RequestVote RPCs to other nodes.
        """
        # Increment current term
        self.currentTerm += 1

        # Vote for self
        self.voted_for = self.nodeId

        # Reset election timeout
        self.reset_election_timeout()

        # Prepare RequestVote request
        request = raft_pb2.RequestVoteRequest(
            term=self.currentTerm,
            candidate_id=self.nodeId,
            last_log_index=self.last_log_index(),
            last_log_term=self.last_log_term()
        )

        # Variables to track votes received
        votes_received = 1  # Vote for self
        votes_needed = (len(self.cluster_nodes) + 1) // 2  # Majority of votes needed

        # Send RequestVote RPC to other nodes
        for node_id, node_address in self.cluster_nodes.items():
            if node_id != self.node_id:
                response = self.send_request_vote(node_address, request)
                if response.vote_granted:
                    votes_received += 1

        # Check if received a majority of votes
        if votes_received > votes_needed:
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
        stub = raft_pb2_grpc.RaftNodeServicesStub(channel)

        # Send RequestVote RPC
        response = stub.RequestVote(request)
        return response


    def become_leader(self):
        """
        Transition to the leader state.
        """
        # Set node state to leader
        self.state = "leader"
        
        # Initialize next_index and match_index for each follower
        self.next_index = {follower_id: self.last_log_index() + 1 for follower_id in self.cluster_nodes}
        self.match_index = {follower_id: 0 for follower_id in self.cluster_nodes}
        
        # Start sending heartbeat messages
        self.start_heartbeat()


    def step_down(self):
        """
        Step down as leader.
        """
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
            return raft_pb2.ServeClientReply(data="", leaderID=self.currentLeader, success=False)
        else:
            # If leader, process the request
            if request.Request.split()[0] == "GET":
                key = request.Request.split()[1]
                return raft_pb2.ServeClientReply(data=self.db[key], success=True)
            
            # Process the SET type request.
            entry = request.Request + " " + self.currentTerm 
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
            term=log_entry.term,
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
                    self.process_appended_entries_response(response)
                except grpc.RpcError as e:
                    # Handle communication errors
                    print(f"Error communicating with node {follower_id}: {e}")


    def process_append_entries_response(self, response):
        """
        Process response of AppendEntries RPC.
        """
        if response.success:
            # If the AppendEntries RPC was successful, update commit_index
            if response.last_log_index > self.commit_index:
                self.commit_index = min(response.last_log_index, len(self.log) - 1)
                # Check if the entry is committed by a majority quorum
                if self.is_majority_committed(self.commit_index):
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
            self.apply_command_to_database(log_entry.command)
            # Update last_applied index
            self.last_applied = index


    def apply_command_to_database(self, command):
        """
        Apply a command to the database.
        """
        # Logic to apply the command to the database
        # For example:
        cmd, key, value = command.split(" ")
        if cmd == "SET":
            self.db.store(key, value)


