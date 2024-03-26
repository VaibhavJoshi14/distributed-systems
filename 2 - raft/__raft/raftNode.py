from __raft import database
import random
from __raft import raft_pb2
from __raft import raft_pb2_grpc
import grpc
import threading
import time
from concurrent import futures
from math import ceil
import os.path

class RaftNode(raft_pb2_grpc.RaftNodeServicesServicer, raft_pb2_grpc.RaftClientServiceServicer):
    def __init__(self, nodeId, db_path, node_address, cluster):
        self.node_address = node_address
        self.cluster_nodes = cluster
        self.db = database.DatabaseKV(db_path) # database object which stores the (key, value) pairs of data, in storage.

        self.meta_file = 'logs_node_' + str(nodeId) + '/metadata.txt' # saves nodeId, currentTerm, votedFor, commitLength
        # the following is stored on stable storage in disk
        
        if os.path.isfile(self.meta_file):
            with open(self.meta_file, 'r') as f:
                lines = f.read().split("\n")
        else:
            # create a new file when it does not exist
            with open(self.meta_file, 'w') as f:
                lines = []
        
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

        if os.path.isfile(self.log_file):
            with open(self.log_file, 'r') as f:
                lines = f.read().split('\n')
        else:
            with open(self.log_file, 'w') as f:
                lines = []
        
        #logs to be stored as: message term\n
        if (len(lines) == 1 and lines[0] == '') or len(lines) == 0:
            self.log = []
        else:
            self.log = lines[:-1]
        
        # the following can be in transient storage, can be lost in a crash with no issue
        self.currentRole = 'follower'  # Possible states: follower, candidate, leader
        self.currentLeader = None
        self.sentLength = [0 for i in self.cluster_nodes]
        self.ackedLength = [0 for i in self.cluster_nodes]
        # assume that the leader has failed after 10 seconds if no heartbeat arrives, since the lease expires 
        # after 10s, and a heartbeat should have come in every 1 to 2 secs. Randomize it: to avoid having lots of 
        # nodes trying to become candidates at the same time.
        self.election_timeout = None
        self.last_leader_communication_time = None
        self.reset_election_timeout()
        
        self.heartbeatInterval = 1 # seconds

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
        
        self.leaderLeaseDuration = 10 # seconds
        self.leaseStartTime = None
        self.wait = False
        self.start_leader_communication_monitoring()


    def writeMetadata(self):
        with open(self.meta_file, 'w') as f:
            f.write("nodeId: " + str(self.nodeId) + "\n" + 
                    "currentTerm: " + str(self.currentTerm) + "\n" +
                    "votedFor: " + str(self.votedFor) + "\n" + 
                    "commitLength: " + str(self.commitLength) + "\n")


    def term(self, entry):
        return int(entry.split()[-1])


    def write_log_to_file(self):
        with open(self.log_file, 'w') as f:
            for line in self.log:
                f.write(line + "\n")


    # When the leader wants to append the log entries (after checking log is ok) to the followers' log, it uses AppendEntries RPC.
    def AppendEntries(self, request, context):
        # Receiving append log request.
        # When the term number in the request is greater than our term number, we accept it as the new term        
        if request.term > self.currentTerm:
            self.currentTerm = request.term
            self.votedFor = None
            self.reset_election_timeout()
            self.writeMetadata()

        if request.term == self.currentTerm:
            self.currentRole = "follower"
            self.currentLeader = request.leaderId
            self.votedFor = None
            self.last_leader_communication_time = time.time()
            self.writeMetadata()
        
        # When
        #     *follower's log length is less than the length of the prefix, then the log is not ok,
        #      implies there are some log entries that the leader did not send us, we have to fill that 
        #      gap first.
        #     *And prefixLength should be either 0, or when it is greater than zero, then we have to look at the 
        #      last log entry in the prefix, its term, that should be equal to the prefixTerm. This guarantees
        #      that the entire log up to and including index prefixTerm will be identical among the follower and 
        #      the leader.
        logOk = len(self.log) >= request.prefixLen and (request.prefixLen == 0 or 
                                                        (request.prefixLen > 0 and self.term(self.log[request.prefixLen - 1]) == request.prefixTerm)
                                                        )
        
        response = raft_pb2.AppendEntriesReply()
        
        # When the log is not ok or the request term is not equal to the current term of the follower, reject the log request
        if not(request.term == self.currentTerm and logOk):
            #print("Not ok", request.term, self.currentTerm, logOk, request.prefixLen, request.prefixTerm, len(self.log))
            #if (request.prefixLen > 0):
                #print("nok", self.term(self.log[request.prefixLen - 1]))
            with open(self.dump_file, 'a') as f:
                f.write(f"Node {self.nodeId} rejected AppendEntries RPC from {request.leaderId}.\n")
                print(f"Node {self.nodeId} rejected AppendEntries RPC from {request.leaderId}.")
            response.success = False
            response.term = self.currentTerm
            return response

        # --------------When the above if condition is false, then process the log request.----------------------------------
        
        # update the lease duration when the appendEntries rpc is accepted, and it is a heartbeat.
        if request.leaseDuration != None:
            self.leaderLeaseDuration = request.leaseDuration
            self.leaseStartTime = time.time()

        # suffix is the new log entries that the leader wants the follower to append
        suffix = list(request.suffix)
        prefixLen = request.prefixLen
        leaderCommit = request.leaderCommit

        # When the follower already has more than prefixLen records, in that case,
        # find the last log entry index between follower and leader state ('index').
        if len(suffix) > 0 and len(self.log) > prefixLen:
            index = min(len(self.log), prefixLen + len(suffix)) - 1
            # for that index, we will compare the term number in the follower's log
            # with the corresponding entry in the suffix (new log entry). If those 
            # are not the same term number, we have an inconsistency in the log, 
            # truncate the log upto prefixLen. Those truncated entries might have come
            # from a previous leader that might not have got commited.
            if self.term(self.log[index]) != self.term(suffix[index - prefixLen]):
                self.log = self.log[ : prefixLen]
                self.write_log_to_file()
            
        
        # Append the new suffix entries (start from what the follower does not already have, upto the end of suffix)
        if prefixLen + len(suffix) > len(self.log):
            for i in range(len(self.log) - prefixLen, len(suffix)):
                self.log.append(suffix[i])
            self.write_log_to_file()
            with open(self.dump_file, 'a') as f:
                f.write(f"Node {self.nodeId} accepted AppendEntries RPC from {request.leaderId}.\n")
                print(f"Node {self.nodeId} accepted AppendEntries RPC from {request.leaderId}.")
            
        # self.commitLength tells how many log entries have been committed so far.
        # If the number of entries commited on leader is greater than the number of entries commited on the 
        # follower, that means, the follower needs to now commit the log entries from indices self.commitLength upto
        # leaderCommit - 1
        if leaderCommit > self.commitLength:
            for i in range(self.commitLength, leaderCommit):
                # In our case, store the SET request on the database
                
                self.commitToDatabase(self.log[i])
                with open(self.dump_file, 'a') as f:
                    f.write(f"Node {self.nodeId} (follower) committed the entry {self.log[i]} to the state machine.\n")
                    print(f"Node {self.nodeId} (follower) committed the entry {self.log[i]} to the state machine.")   
            
            # set self.commitLength equal to leaderCommit since we have committed the uncommitted entries as
            # suggested by leader.
            self.commitLength = leaderCommit
            self.writeMetadata()
        
        
        response.success = True
        # ack tells how many log entries from the start have been successfully received by the follower.
        response.ack = prefixLen + len(suffix)
        response.senderId = self.nodeId
        response.term = self.currentTerm
        return response



    def reset_election_timeout(self):
        """
        Reset the election timeout.
        """
        # Adjust these values based on your requirements
        min_timeout = 15000   # in milliseconds
        max_timeout = 21000   # in milliseconds
        self.election_timeout = random.randint(min_timeout, max_timeout) / 1000  # Convert to seconds
        self.last_leader_communication_time = time.time()


    def RequestVote(self, request, context):
        # if candidate's term is greater than our term, we set the term as the candidate's term,
        # transition to follower state.
        if request.cTerm > self.currentTerm:
            self.currentTerm = request.cTerm
            self.currentRole = "follower"
            self.votedFor = None
            self.writeMetadata()
        
        self.reset_election_timeout()

        lastTerm = 0
        if len(self.log) > 0:
            lastTerm = self.term(self.log[-1])
        
        # we don't want a leader which has an outdated log.
        logOk = request.cLogTerm > lastTerm or (request.cLogTerm == lastTerm and request.cLogLength >= len(self.log))

        if request.cTerm == self.currentTerm and logOk and self.votedFor in {request.cId, None} and self.wait == False:
            self.votedFor = request.cId
            self.writeMetadata()
            with open(self.dump_file, 'a') as f:
                f.write(f"Vote granted for Node {request.cId} in term {self.currentTerm}.\n")
                print(f"Vote granted for Node {request.cId} in term {self.currentTerm}.")
            
            if self.leaseStartTime != None:
                oldLease = int(ceil(time.time() - self.leaseStartTime)) 
            else:
                oldLease = 0
            
            return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=True, oldLeaderRemainingLease = oldLease)
        
        else:
            with open(self.dump_file, 'a') as f:
                f.write(f"Vote denied for Node {request.cId} in term {self.currentTerm}.\n")
                print(f"Vote denied for Node {request.cId} in term {self.currentTerm}.")
            
            return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=False)
            


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


    def send_heartbeats(self):
        """
        Send periodic heartbeats to followers.
        """
        # reset the votedFor metadata
        self.votedFor = None
        self.writeMetadata()
        
        need = int(ceil((len(self.cluster_nodes) + 1) / 2))

        while self.currentRole == "leader":  
            with open(self.dump_file, 'a') as f:
                f.write(f"Leader {self.nodeId} sending heartbeat & Renewing Lease\n")
                print(f"Leader {self.nodeId} sending heartbeat & Renewing Lease")
            
            self.reset_election_timeout()
            
            acq = 1

            # Send AppendEntries RPC to the followers
            for follower_id in self.cluster_nodes:
                if follower_id != self.nodeId:
                    response = self.replicateLog(self.nodeId, follower_id, True)
                    if response != None and response.success == True:
                        acq += 1
            
            if acq >= need:
                # renew the lease, if it gets majority votes
                self.leaderLeaseDuration = 10 # seconds
                self.leaseStartTime = time.time()
            else:
                # step down and become a follower
                self.currentRole = "follower"
                with open(self.dump_file, 'a') as f:
                    f.write(f"Leader {self.nodeId} lease renewal failed. Stepping Down.\n")
                    print(f"Leader {self.nodeId} lease renewal failed. Stepping Down.")
                self.reset_election_timeout()
                
                # wait for the remaining duration of lease, so that others could GET.
                curr_time = time.time()
                if (curr_time - self.leaseStartTime < self.leaderLeaseDuration):
                    time.sleep(self.leaderLeaseDuration - (curr_time - self.leaseStartTime))
                
                break

            # Sleep for the heartbeat interval
            time.sleep(self.heartbeatInterval)
        


    def start_election(self):
        """
        Start a new election by sending RequestVote RPCs to all nodes.
        """
        self.currentTerm += 1
        self.votedFor = self.nodeId
        self.writeMetadata()
        
        request = raft_pb2.RequestVoteMsg(
            cId = self.nodeId,
            cTerm = self.currentTerm,
            cLogLength = len(self.log),
            cLogTerm = self.last_log_term()
        )

        votesReceived = set()
        votesReceived.add(self.nodeId)
        votesNeeded = int(ceil((len(self.cluster_nodes) + 1) / 2)) # Majority of votes needed
        maxRemainingLeaderLease = -1
        timeLease = -1
        # Send RequestVote RPC to other nodes
        for node_id, node_address in self.cluster_nodes.items():
            if node_id != self.nodeId:
                
                with grpc.insecure_channel(node_address) as channel:
                    try:
                        stub = raft_pb2_grpc.RaftNodeServicesStub(channel)
                        response = stub.RequestVote(request)
                    except grpc.RpcError as e:
                        response = None
                
                if response == None:
                   # print("none")
                    continue
                
                if response.voteGranted == True and maxRemainingLeaderLease < response.oldLeaderRemainingLease and response.oldLeaderRemainingLease > 0:
                    maxRemainingLeaderLease = response.oldLeaderRemainingLease
                    timeLease = time.time()

                if self.currentRole == "candidate" and response.term == self.currentTerm and response.voteGranted == True:
                    votesReceived.add(node_id)
                
                #print(votesNeeded, votesReceived)
                if len(votesReceived) >= votesNeeded:
                    self.currentRole = "leader"
                    self.currentLeader = self.nodeId
                    
                    # wait for max remaining old leader lease.
                    curTime = time.time()
                    if curTime - timeLease < maxRemainingLeaderLease and maxRemainingLeaderLease > 0:
                        with open(self.dump_file, 'a') as f:
                            f.write(f"New Leader waiting for Old Leader Lease to timeout.\n")
                            print(f"New Leader waiting for Old Leader Lease to timeout.")
                        self.wait = True
                        time.sleep(maxRemainingLeaderLease - (curTime - timeLease))
                        self.wait = False
                    
                    # start own lease
                    self.leaseStartTime = time.time()
                    self.reset_election_timeout()
        
                    with open(self.dump_file, 'a') as f:
                        f.write(f"Node {self.nodeId} became the leader for term {self.currentTerm}.\n")
                        print(f"Node {self.nodeId} became the leader for term {self.currentTerm}.")
                    

                    for fid in self.cluster_nodes:
                        if fid != self.nodeId:
                            self.sentLength[fid] = len(self.log)
                            self.ackedLength[fid] = 0
                            self.replicateLog(self.nodeId, fid, False)

                    entry = "NO-OP " + str(self.currentTerm)
                    self.log.append(entry)
                    # save the entry to file
                    with open(self.log_file, 'a') as f:
                        f.write(entry + "\n")
        
                    self.ackedLength[self.nodeId] = len(self.log)
        
                    for fid in self.cluster_nodes:
                        if fid != self.nodeId:
                            self.replicateLog(self.nodeId, fid, False)

                    # Start sending heartbeat messages
                    self.start_heartbeat()
                    break

                elif response.term > self.currentTerm:
                    self.currentTerm = response.term
                    self.currentRole = "follower"
                    self.votedFor = None
                    self.reset_election_timeout()
                    self.writeMetadata()



    def ServeClient(self, request, context):
        """
        Handle client requests.
        """
        if self.currentRole != "leader":
            return raft_pb2.ServeClientReply(data="Fail: Not a leader", leaderId=self.currentLeader, success=False)
        
        
        with open(self.dump_file, 'a') as f:
            f.write(f"Node {self.nodeId} (leader) received an {request.Request} request.\n")
            print(f"Node {self.nodeId} (leader) received an {request.Request} request.")

        # If leader, process the request
        # If it is a GET request, do it if the lease has not expired.
        if request.Request.split()[0] == "GET" and time.time() - self.leaseStartTime <= self.leaderLeaseDuration:
            key = request.Request.split()[1]
            return raft_pb2.ServeClientReply(data=self.db.get(key), success=True)
            
        # Append the set type request to leader's log
        entry = request.Request + " " + str(self.currentTerm) 
        self.log.append(entry)

        # save the entry to file
        with open(self.log_file, 'a') as f:
            f.write(entry + "\n")
        
        # the leader itself acknowledges the delivery of message
        
        self.ackedLength[self.nodeId] = len(self.log)
        
        for fid in self.cluster_nodes:
            if fid != self.nodeId:
                self.replicateLog(self.nodeId, fid, False)

        return raft_pb2.ServeClientReply(success=True)



    # This is the log append request sending rpc.
    def replicateLog(self, leaderId, followerId, isHeartbeat):
        
        if isHeartbeat == True:
            _leaseDuration = self.leaderLeaseDuration
        else:
            _leaseDuration = None

        prefixLen = self.sentLength[followerId]
        # all remaining log entries that need to be sent
        suffix = self.log[prefixLen:]
        
        prefixTerm = 0
        if prefixLen > 0:
            # look at the term in the last sent log.
            prefixTerm = self.term(self.log[prefixLen-1])
        
        # ------------------Send to the follower------------------------------------
        request = raft_pb2.AppendEntriesMsg(
                    prefixLen = prefixLen,
                    leaderCommit = self.commitLength,
                    suffix = suffix,
                    leaderId = leaderId,
                    term = self.currentTerm,
                    prefixTerm = prefixTerm,
                    leaseDuration = _leaseDuration
                )
        response = None
        # Establish gRPC channel to the follower
        #print("sending at ", followerId, self.cluster_nodes[followerId])
        with grpc.insecure_channel(self.cluster_nodes[followerId]) as channel:
            stub = raft_pb2_grpc.RaftNodeServicesStub(channel)
            try:
                # Send AppendEntries RPC
                response = stub.AppendEntries(request)
                # After leader gets the response, the leader needs to decide which log entries are ready 
                # to be commited.
                self.processLogResponse(response)
            except grpc.RpcError as e:
                with open(self.dump_file, 'a') as f:
                    f.write(f"Error occurred while sending RPC to Node {followerId}.\n")
                    print(f"Error occurred while sending RPC to Node {followerId}.")
        return response


    def processLogResponse(self, response):
        # A log entry is ready to be committed if it is acknowledged by a quorum of raft nodes.
        
        self.reset_election_timeout()
        
        if response.term == self.currentTerm and self.currentRole == "leader":
            
            if response.success == True and response.ack >= self.ackedLength[response.senderId]:
                self.sentLength[response.senderId] = response.ack
                self.ackedLength[response.senderId] = response.ack
                self.commitLogEntries()
            # if not successfull, it could be that there was a gap, we decrement the sentLength[follower] variable,
            # and send one more log entry to the follower. This could take a number of iterations to do this.
            elif self.sentLength[response.senderId] > 0:
                self.sentLength[response.senderId] = self.sentLength[response.senderId] - 1
                self.replicateLog(self.nodeId, response.senderId, False)
        # when the follower is at a higher term, then the leader should shift to a 'follower' state.
        elif response.term > self.currentTerm:
            self.currentTerm = response.term
            self.currentRole = "follower"
            self.votedFor = None
            self.reset_election_timeout()
            self.writeMetadata()
            with open(self.dump_file, 'a') as f:
                f.write(f"{self.nodeId} Stepping down\n")
                print(f"{self.nodeId} Stepping down")

        self.reset_election_timeout()


    def commitLogEntries(self):
        # A log entry is ready to be committed if it is acknowledged by a quorum of raft nodes.
        while self.commitLength < len(self.log):
            acks = 0
            
            for node in range(len(self.cluster_nodes)):
                if self.ackedLength[node] > self.commitLength:
                    acks += 1
            
            if acks >= int(ceil((len(self.cluster_nodes) + 1) / 2)):
                # deliver the message to the application
                self.commitLength += 1
                committed = self.commitToDatabase(self.log[self.commitLength-1])
                self.writeMetadata() # to update the commitLength in file
                
                with open(self.dump_file, 'a') as f:
                    f.write(f"Node {self.nodeId} (leader) committed the entry {committed} to the state machine.\n")
                    print(f"Node {self.nodeId} (leader) committed the entry {committed} to the state machine.")  
                
            else:
                break



    def commitToDatabase(self, command):
        if "SET" in command:
            cmd, key, value, term = command.split()
            self.db.store(key, value)
            return cmd + " " + key + " " + value
        else:
            return command


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
            if self.currentRole == "leader":
                self.reset_election_timeout()
                #current_time = time.time()
                #print("Times ", current_time-self.last_leader_communication_time, self.election_timeout)
                time.sleep(self.leader_check_interval)
                continue
            current_time = time.time()

            if current_time - self.last_leader_communication_time > self.election_timeout:
                # if this condition is satisfied, assume that leader has failed, and start a new election.
                               
                # Save to dump file that a new election is being started
                with open(self.dump_file, 'a') as f:
                    f.write(f"Node {self.nodeId} election timer timed out, Starting election.\n")
                    print(f"Node {self.nodeId} election timer timed out, Starting election.")

                self.currentRole = "candidate"
                self.reset_election_timeout()
                self.start_election()
                
            time.sleep(self.leader_check_interval)



    def join(self):
        while (self.communication_monitor_thread and self.communication_monitor_thread.is_alive()) or \
            (self.heartbeat_thread and self.heartbeat_thread.is_alive()):
            if self.communication_monitor_thread:
                self.communication_monitor_thread.join(1)
            if self.heartbeat_thread:
                self.heartbeat_thread.join(1)
        
        self.server.wait_for_termination()
