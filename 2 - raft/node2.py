from __raft import raftNode

nodeId = 2
nodeaddr = "localhost" + ":" + "50053"
cluster_nodes = {0: "localhost:50051", 1:"localhost:50052", 2: nodeaddr, 3: "localhost:50054", 4: "localhost:50055"}
db_path = 'data/data' + str(nodeId) + '.txt' # to store the key-value pairs of data

node = raftNode.RaftNode(nodeId, db_path, nodeaddr, cluster_nodes)
node.join()
