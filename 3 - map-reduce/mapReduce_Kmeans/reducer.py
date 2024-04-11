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
        
        port = self.selfAddress.split(":")[1]
        self.server.add_insecure_port("[::]:" + port)
        self.server.start()
        print("Server started, listening on " + port)



    def join(self):
        self.server.wait_for_termination()

