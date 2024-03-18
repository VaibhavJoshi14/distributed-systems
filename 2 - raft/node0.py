from __raft import database, raftNode

nodeId = 0
nodeaddr = "localhost" + ":" + "50051"
cluster_nodes = {0: nodeaddr, 1: "localhost:50052", 2: "localhost:50053"}
db_path = 'data/data' + str(nodeId) + '.txt' # to store the key-value pairs of data

db = database.DatabaseKV(db_path)
node = raftNode.RaftNode(nodeId, db_path, nodeaddr, cluster_nodes)
node.join()

