from __raft import database, raftNode

nodeId = 1
nodeaddr = "localhost"
db_path = 'data' + str(nodeId) + '.txt' # to store the key-value pairs of data

db = database.DatabaseKV(db_path)
node = raftNode.RaftNode(nodeId, db_path, nodeaddr)

