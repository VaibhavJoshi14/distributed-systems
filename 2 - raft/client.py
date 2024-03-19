from __raft import raft_pb2, raft_pb2_grpc

client_addr = "localhost:50050"
# addresses of each raft node
cluster_nodes = {0: "localhost:50051", 1: "localhost:50052", 2: "localhost:50053"}

class Client:
    def __init__(client_addr, cluster_nodes):
        self.client_addr = client_addr
        self.cluster_nodes = cluster_nodes
        self.leaderId = None # current raft leader id, can get stale
    
    # ask raft for value corresponding to 'key'
    #def get(self, key):


    def set(self, key, value):
        with grpc.insecure_channel(self.cluster_nodes[self.leaderId]) as channel:
            stub = raft_pb2_grpc.RaftClientServiceStub(channel)
            response = stub.ServeClient(raft_pb2.SearchRequest())

if __name__ == "__main__":
    client = Client(client_addr, cluster_nodes)
    # create a menu
    client.set("name1", "Gaurav")
    #client.get("name1")