from __raft import raft_pb2, raft_pb2_grpc
import grpc 
import time

client_addr = "localhost:50050"
# addresses of each raft node
cluster_nodes = {0: "localhost:50051", 1: "localhost:50052", 2: "localhost:50053", 3: "localhost:50054", 4: "localhost:50055"}

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
                try:
                    response = stub.ServeClient(raft_pb2.ServeClientArgs(Request = req))
                    if response.success == True:
                        print(f"GET request succeeded. Value corresponding to key {key} is {response.data}")
                        break
                    else:
                        print(response.data)
                        if response.leaderId == None:
                            print("No leader in the raft system.")
                except grpc.RpcError as e:
                    # when the assumed leader dies
                    self.leaderId = (self.leaderId + 1) % len(self.cluster_nodes)
            
            # change the currentLeaderId here because that would be the reason of failure
            if (self.leaderId != None):
                self.leaderId = response.leaderId    


    def set(self, key, value):
        while True:
            # keep trying until request succeeds
            with grpc.insecure_channel(self.cluster_nodes[self.leaderId]) as channel:
                req = "SET " + key + " " + value
                stub = raft_pb2_grpc.RaftClientServiceStub(channel)
                response = None
                try:
                    response = stub.ServeClient(raft_pb2.ServeClientArgs(Request = req))
                    if response.success == True:
                        print("SET request succeeded.")
                        break
                    else:
                        print(response.data)
                        if response.leaderId == None:
                            print("No leader in the raft system.")
                except grpc.RpcError as e:
                    # when the assumed leader dies
                    self.leaderId = (self.leaderId + 1) % len(self.cluster_nodes)

            # change the currentLeaderId here because that would be the reason of failure
            if (response != None and self.leaderId != None):
                self.leaderId = response.leaderId    


if __name__ == "__main__":
    client = Client(client_addr, cluster_nodes)

    while True:
        print("\n--------------------------------")
        print("1. SET request")
        print("2. GET request")
        print("--------------------------------")
        x = int(input("Enter: "))
        if x == 1:
            # Type SET key value
            entry = input("Query: ")
            _, k, v = entry.split()
            client.set(k, v)
        elif x == 2:
            # Type GET key
            entry = input("Query: ")
            _, k = entry.split()
            client.get(k)