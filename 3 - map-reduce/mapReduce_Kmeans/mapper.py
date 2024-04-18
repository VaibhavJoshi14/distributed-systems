import pandas as pd
import numpy as np
import grpc
from mapReduce_Kmeans import map_reduce_kmeans_pb2, map_reduce_kmeans_pb2_grpc
import threading
from concurrent import futures

class Mapper(map_reduce_kmeans_pb2_grpc.MapperServicer):
    def __init__(self, selfAddress, selfId, masterAddress):
        self.selfAddress = selfAddress
        self.selfId = selfId
        self.masterAddress = masterAddress

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
        self.partitions = None


    # This is the rpc that is called by the master for starting a map task
    def Map(self, request, context):
        print("-------------------------------------------")
        if self.currentFile == None:
            self.currentFile = request.inputFile
            if request.dfHasHeader == True:
                self.df = pd.read_csv(request.inputFile, sep=request.dfSep).iloc[request.inputStartIndex: request.inputEndIndex + 1, :]
            else:
                self.df = pd.read_csv(request.inputFile, header = None, sep=request.dfSep).iloc[request.inputStartIndex: request.inputEndIndex + 1, :]
            self.numReducers = request.numReducers
        
        self.currentRequest = request
        
        return map_reduce_kmeans_pb2.Reply(message="Ok")


    def __map(self):
        while True:
            # will this always work
            if self.currentRequest == None:
                continue
            # Else procees the request
            Keys = [0 for i in range(len(self.df))]
            
            # Convert the centroids from grpc message format to list format
            centroids = self.getCentroids(self.currentRequest.centroids)
            print("Received centroids ", centroids)
            
            # Assign each point of the data to the nearest centroid.  
            idx = 0
            for index, dataPoint in self.df.iterrows():
                Keys[idx] = self.assign_nearest_centroid(list(dataPoint), centroids)
                idx += 1

            self.partition(Keys=Keys, Values=self.df.values.tolist())

            self.currentRequest = None


    def partition(self, Keys, Values):
        # All key-value pairs with the same key are sent to the same partition.
        
        self.partitions = [[] for i in range(self.numReducers)]
        for i in range(len(Keys)):
            self.partitions[Keys[i]].append(Values[i])

        print("The partitions are ")
        for i in range(self.numReducers):
            with open("Mappers/M" + str(self.selfId) + "/partition_" + str(i+1), 'a') as f:
                f.write(self.partitions[i].__str__())
                f.write("\n-----------------------------------------------------------------------------------------\n")
            print(self.partitions[i])
        
        
        # Send to the master that it has done its job.
        with grpc.insecure_channel(self.masterAddress) as channel:
            stub = map_reduce_kmeans_pb2_grpc.MasterServicesStub(channel)
            response = stub.MapResponse(map_reduce_kmeans_pb2.Empty(id = self.selfId))


    def getNumDistinctValues(self, Keys):
        return len(set(Keys))


    def euclidean_distance(self, point1, point2):
        return np.linalg.norm(np.array(point1) - np.array(point2))


    # assign the nearest centroid to a data point from the list of centroids, using Euclidean distance.
    def assign_nearest_centroid(self, dataPoint, centroids):
        nearest = 0
        nearest_distance = self.euclidean_distance(dataPoint, centroids[0])
        #print(centroids)
        for i in range(1, len(centroids)):
            if centroids[i] != []:
                dist = self.euclidean_distance(dataPoint, centroids[i])
                if dist < nearest_distance:
                    nearest = i
                    nearest_distance = dist
        return nearest


    def getCentroids(self, centroids):
        centroids = list(centroids)
        _centroids = []
            
        for j in range(len(centroids)):
            _centroids.append(centroids[j].data)

        return _centroids


    # This rpc is called by each reducer to get their share of reduce job.
    def GetInputFromMapper(self, request, context):
        idx = request.reducerId - 1
        
        toSend = self.partitions[idx]
        
        response = map_reduce_kmeans_pb2.KeyValueDataList()
        for msg in toSend:
            resp = map_reduce_kmeans_pb2.KeyValueData()
            resp.key = idx
            resp.data.extend(msg)
            response.data.append(resp) 
        
        return response


    def join(self):
        while (self.mapThread.is_alive()):
            self.mapThread.join(1)
        self.server.wait_for_termination()