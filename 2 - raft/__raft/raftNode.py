import database.py

class RaftNode:
    def __init__(self, node_id, db):
        self.db = db # database object which stores the (key, value) pairs of data, in storage as well as transiently.

         # the following to be stored on stable storage in disk
        self.node_id = node_id
        self.currentTerm = 0
        self.votedFor = None
        self.commitLength = 0
        self.log = [] # transient copy of log

        self.log_file = 'logs_node_' + str(node_id) + '/logs.txt' # log saved on file
        self.meta_file = 'logs_node_' + str(node_id) + '/metadata.txt' # saves nodeId, currentTerm, votedFor, commitLength

        # the following can be in transient storage, can be lost in a crash with no issue
        self.currentRole = 'follower'  # Possible states: follower, candidate, leader
        self.currentLeader = None
        self.votesReceived = []
        self.sentLength = []
        self.ackedLength = []

"""  def request_vote(self, term, candidate_id, last_log_index, last_log_term):
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

