from __raft import raftNode

nodeId = 4
nodeaddr = "localhost"
db_path = 'data' + str(nodeId) + '.txt' # to store the key-value pairs of data

node = raftNode.RaftNode(nodeId, db_path, nodeaddr)

