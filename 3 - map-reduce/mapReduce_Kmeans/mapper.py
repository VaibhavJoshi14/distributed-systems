import pandas as pd
import numpy as np
import grpc
from mapReduce_Kmeans import map_reduce_kmeans_pb2, map_reduce_kmeans_pb2_grpc
import threading
from concurrent import futures

class Mapper(map_reduce_kmeans_pb2_grpc.MapperServicer):
    def __init__(self, selfAddress):
        self.selfAddress = selfAddress

        # initialize the grpc server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        map_reduce_kmeans_pb2_grpc.add_MapperServicer_to_server(self, self.server)
        
        port = self.selfAddress.split(":")[1]
        self.server.add_insecure_port("[::]:" + port)
        self.server.start()
        print("Server started, listening on " + port)

        # These two should be reset after completion of the kmeans algorithm. Maybe a flush rpc
        # to be sent by the master.
        self.currentFile = None # current file to work on
        self.df = None # current split of the dataframe
        self.numReducers = None
        self.currentRequest = None
        
        self.mapThread = threading.Thread(target=self.__map)
        self.mapThread.daemon = True
        self.mapThread.start()


    # This is the rpc that is called by the master for starting a map task
    def Map(self, request, context):
        if self.currentFile != None:
            self.currentFile = request.inputFile
            if request.dfHasHeader == True:
                self.df = pd.read_csv(request.inputFile)[request.inputStartIndex: request.inputEndIndex + 1, :]
            else:
                self.df = pd.read_csv(request.inputFile, header = None)[request.inputStartIndex: request.inputEndIndex + 1, :]
            self.numReducers = request.numReducers

        self.currentRequest = request
        
        return map_reduce_kmeans_pb2.Reply(message="Ok")


    def __map(self):
        while True:
            # will this always work
            if self.currentRequest == None:
                continue
            # Else procees the request
            centroids = self.currentRequest.centroids

    
    def join(self):
        while (self.mapThread.is_alive()):
            self.mapThread.join(1)
        self.server.wait_for_termination()