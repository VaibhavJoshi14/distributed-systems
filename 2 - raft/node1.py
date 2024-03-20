from __raft import raftNode

nodeId = 1
nodeaddr = "localhost" + ":" + "50052"
cluster_nodes = {0: "localhost:50051", 1: nodeaddr, 2:"localhost:50053"}
db_path = 'data/data' + str(nodeId) + '.txt' # to store the key-value pairs of data

node = raftNode.RaftNode(nodeId, db_path, nodeaddr, cluster_nodes)
node.join()
