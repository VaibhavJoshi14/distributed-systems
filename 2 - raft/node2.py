from __raft import database, raftNode

nodeId = 2
nodeaddr = "localhost" + ":" + "50053"
cluster_nodes = {0: "localhost:50051", 1:"localhost:50052", 2: nodeaddr}
db_path = 'data/data' + str(nodeId) + '.txt' # to store the key-value pairs of data

db = database.DatabaseKV(db_path)
node = raftNode.RaftNode(nodeId, db_path, nodeaddr, cluster_nodes)
node.join()
