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
            
            #reset, for next iteration.
            self.startWorking = False
            self.data = []
            self.keys = []


    def extractDataFromResponse(self, response):
        response = response.data
        for dt in response:
            self.data.append(dt.data)
            self.keys.append(dt.key)

    def join(self):
        self.server.wait_for_termination()

