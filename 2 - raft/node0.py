from __raft import database, raftNode

nodeId = 0
nodeaddr = "localhost"
cluster_nodes = {0: "localhost", 1: "localhost"}
db_path = 'data' + str(nodeId) + '.txt' # to store the key-value pairs of data

db = database.DatabaseKV(db_path)
node = raftNode.RaftNode(nodeId, db_path, nodeaddr, cluster_nodes)
node.join()

