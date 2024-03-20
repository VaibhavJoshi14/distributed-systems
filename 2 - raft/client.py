from __raft import raft_pb2, raft_pb2_grpc
import grpc 

client_addr = "localhost:50050"
# addresses of each raft node
cluster_nodes = {0: "localhost:50051", 1: "localhost:50052", 2: "localhost:50053"}

class Client:
    def __init__(self, client_addr, cluster_nodes):
        self.client_addr = client_addr
        self.cluster_nodes = cluster_nodes
        self.leaderId = 0 # give it arbitrary initial value; # current raft leader id, can get stale
    
    # ask raft for value corresponding to 'key'
    def get(self, key):
        while True:
            # keep trying until request succeeds
            with grpc.insecure_channel(self.cluster_nodes[self.leaderId]) as channel:
                req = "GET " + key
                stub = raft_pb2_grpc.RaftClientServiceStub(channel)
                response = stub.ServeClient(raft_pb2.ServeClientArgs(Request = req))
                if response.success == True:
                    print(f"GET request succeeded. Value corresponding to key {key} is {response.data}")
                    break
                else:
                    print(response.data)
                    if response.leaderId == None:
                        print("No leader in the raft system.")
            
            # change the currentLeaderId here because that would be the reason of failure
            if (self.leaderId != None):
                self.leaderId = response.leaderId    


    def set(self, key, value):
        while True:
            # keep trying until request succeeds
            with grpc.insecure_channel(self.cluster_nodes[self.leaderId]) as channel:
                req = "SET " + key + " " + value
                stub = raft_pb2_grpc.RaftClientServiceStub(channel)
                response = stub.ServeClient(raft_pb2.ServeClientArgs(Request = req))
                if response.success == True:
                    print("SET request succeeded.")
                    break
                else:
                    print(response.data)
                    if response.leaderId == None:
                        print("No leader in the raft system.")
            # change the currentLeaderId here because that would be the reason of failure
            if (self.leaderId != None):
                self.leaderId = response.leaderId    


if __name__ == "__main__":
    client = Client(client_addr, cluster_nodes)
    # create a menu
    client.set("name1", "Gaurav")
    client.get("name1")

