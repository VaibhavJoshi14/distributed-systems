from __raft import database

class RaftNode:
    def __init__(self, nodeId, db):
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


    def writeMetadata(self):
        with open(self.meta_file, 'w') as f:
            f.write("nodeId: " + str(self.nodeId) + "\n" + 
                    "currentTerm: " + str(self.currentTerm) + "\n" +
                    "votedFor: " + str(self.votedFor) + "\n" + 
                    "commitLength: " + str(self.commitLength) + "\n")


"""  def requestVote(self, term, candidate_id, last_log_index, last_log_term):
        # Logic for processing RequestVote RPC

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        # Logic for processing AppendEntries RPC

    def send_heartbeat(self):
        # Logic for sending heartbeat to followers

    def become_follower(self, term, leader_id):
        self.state = 'follower'
        self.current_term = term
        self.voted_for = None
        self.leader_id = leader_id

    def become_candidate(self):
        # Logic for transitioning to candidate state

    def become_leader(self):
        # Logic for transitioning to leader state

    def handle_client_request(self, key, value):
        # Logic for handling client requests to store data

    def get_value(self, key):
        # Logic for retrieving value from the local database"""

