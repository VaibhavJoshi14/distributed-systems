import pandas as pd
import numpy as np
import grpc
from mapReduce_Kmeans import map_reduce_kmeans_pb2, map_reduce_kmeans_pb2_grpc
import threading
from concurrent import futures

class Reducer(map_reduce_kmeans_pb2_grpc.ReducerServicer):
    def __init__(self, selfAddress, selfId, masterAddress, mapperAddresses):
        self.selfAddress = selfAddress
        self.selfId = selfId
        self.masterAddress = masterAddress
        self.mapperAddresses = mapperAddresses

         # initialize the grpc server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        map_reduce_kmeans_pb2_grpc.add_ReducerServicer_to_server(self, self.server)
        
        self.startWorking = False
        self.data = []
        self.keys = []
        self.reduceThread = threading.Thread(target=self.ShuffleAndSort)
        self.reduceThread.daemon = True
        self.reduceThread.start()

        port = self.selfAddress.split(":")[1]
        self.server.add_insecure_port("[::]:" + port)
        self.server.start()
        print("Server started, listening on " + port)


    # This rpc is sent by the master to tell it to start communicating with each mapper
    # to get their reduce tasks.
    def ReduceInit(self, request, context):
        print("-------------------------------------------")
        print("Reducer", self.selfId, "received ReduceInit request")
        self.startWorking = True # leads to starting of Shuffle and Sort.
        return map_reduce_kmeans_pb2.Reply(message="Ok")


    def ShuffleAndSort(self):
        while True:
            if self.startWorking == False:
                continue
            # The reducer now calls each mapper to get its work.
            for i in range(len(self.mapperAddresses)):
                print("Reducer", self.selfId, "sending request to mapper", i+1, "to get its job.")
                with grpc.insecure_channel(self.mapperAddresses[i]) as channel:
                    stub = map_reduce_kmeans_pb2_grpc.MapperStub(channel)
                    response = stub.GetInputFromMapper(map_reduce_kmeans_pb2.ReduceInputRequest(reducerId=self.selfId)) # will give ids starting from 1.
                    self.extractDataFromResponse(response)
            
            print(self.data)
            print(self.keys)

            #reduce
            self.ReduceFunc()

            #reset, for next iteration.
            self.startWorking = False
            self.data = []
            self.keys = []


    def extractDataFromResponse(self, response):
        response = response.data
        for dt in response:
            self.data.append(dt.data)
            self.keys.append(dt.key)


    def ReduceFunc(self):
        # Perform pairwise summation over the values for each unique key
        unique_keys = set(self.keys)
        results = {}
        for key in unique_keys:
            indices = [i for i, k in enumerate(self.keys) if k == key]
            values = [self.data[i] for i in indices]
            summed_value = np.sum(values, axis=0)
            results[key] = summed_value / len(self.keys)
        print(results)

        print("Reducer", self.selfId, "sending response to master.")

        # Prepare data for master
        response_list = map_reduce_kmeans_pb2.KeyValueDataList()
        response_list.id = self.selfId

        # Populate response_list with key-value pairs from the results
        for key, value in results.items():
            data_entry = map_reduce_kmeans_pb2.KeyValueData()
            data_entry.key = key
            data_entry.data.extend(value)
            response_list.data.append(data_entry)
        
        # print(response_list.data)
        # print(response_list)

        # Send data to master 
        with grpc.insecure_channel(self.masterAddress) as channel:
            stub = map_reduce_kmeans_pb2_grpc.MasterServicesStub(channel)
            response = stub.ReduceResponse(response_list)
            print("Response from master: ",response.message)
        
        # Write the result to a file specific to the reducer's ID
        output_file = f"Reducers/R{self.selfId}.txt"
        
        try :
            with open(output_file, "a") as f:
                for key, value in results.items():
                    f.write(f"Key: {key}, Mean Value: {value}\n")
                f.close()
        except:
            with open(output_file, "w") as f:
                for key, value in results.items():
                    f.write(f"Key: {key}, Mean Value: {value}\n")
                f.close()
    

    def join(self):
        self.server.wait_for_termination()

