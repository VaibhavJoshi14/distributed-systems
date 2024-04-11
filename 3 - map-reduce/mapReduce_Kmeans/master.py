import pandas as pd
import numpy as np
import grpc
from mapReduce_Kmeans import map_reduce_kmeans_pb2, map_reduce_kmeans_pb2_grpc
import threading
from concurrent import futures

class Master(map_reduce_kmeans_pb2_grpc.MapperResponseServicer):
    def __init__(self, selfAddress, mapperAddresses, reducerAddresses):
        self.selfAddress = selfAddress
        self.mapperAddresses = mapperAddresses
        self.reducerAddresses = reducerAddresses
        self.numMappers = len(mapperAddresses)
        self.numReducers = len(reducerAddresses)

        # initialize the grpc server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        map_reduce_kmeans_pb2_grpc.add_MapperResponseServicer_to_server(self, self.server)
        
        port = self.selfAddress.split(":")[1]
        self.server.add_insecure_port("[::]:" + port)
        self.server.start()
        print("Server started, listening on " + port)


    # Inputs to the function kmeans:
    #    df: dataframe (pandas) to cluster.
    #    inputFile: path of file where df is stored.
    #    k: number of centroids (integer).
    #    maxIter: maximum iterations to run the algorithm for (integer).
    #    maxAttempts: maximum retries to make if the clusters returned are less than k (integer).
    def kmeans(self, df, inputFile, k, maxIter=100, maxAttempts=100, dfHasHeader=True):
        # Step 1: Randomly initialize k cluster centroids.
        centroids = self.init_centroids(df, k)

        # will contain the clusterId assigned to each point.
        clusterId = [0 for i in range(len(df))]

        _centroids = []
        max_iter = maxIter

        # Produce splits, according to the number of mappers
        part = int(len(df) / self.numMappers)
        splits = [[part * i, part * (i+1) - 1] for i in range(self.numMappers - 1)]
        splits.append([part * (self.numMappers - 1), len(df) - 1])
        print("Splits on data of size ", len(df), "for numMappers ", self.numMappers, " are ", splits)
        
        # Step 2: Repeat until the centroids converge or the max iter limit has reached.
        while True:
            
            # Converting centroids to the grpc proto datatype.
            _centroids_ = []
            for i in range(len(centroids)):
                _centroids_.append(map_reduce_kmeans_pb2.Data(data=centroids[i]))

            # Send the Map rpc to each mapper to work on their splits.
            for i in range(self.numMappers):
                request = map_reduce_kmeans_pb2.MapRequest(
                    inputStartIndex = splits[i][0],
                    inputEndIndex = splits[i][1],
                    inputFile = inputFile,
                    dfHasHeader = dfHasHeader,
                    centroids = _centroids_,
                    numReducers = self.numReducers
                )

                with grpc.insecure_channel(self.mapperAddresses[i]) as channel:
                    stub = map_reduce_kmeans_pb2_grpc.MapperStub(channel)
                    # it just sends the request, and the Mapper immediately
                    # replies with Ok, and the mapper starts the job. The 
                    # job is thus done parallely by each mapper, which reply
                    # later after completion to the master.
                    response = stub.Map(request)
            

            """# Step 2.1: Assign each point of the data to the nearest centroid.  
            for index, dataPoint in df.iterrows():
                clusterId[index] = assign_nearest_centroid(list(dataPoint), centroids)

            # Step 2.2: Recompute the centroid of each cluster
            _centroids = compute_cluster_centroids(df, clusterId, k)"""
        

            max_iter -= 1
            if (max_iter > 0 and centroids != _centroids) == False:
                break
            
            centroids = self.preprocess(_centroids.copy(), df, k)

            # retry when lesser clusters are identified. This depends on the random initialization.
            if len(centroids) < k and maxAttempts > 0:
                return self.kmeans(df, inputFile, k, maxIter, maxAttempts-1, dfHasHeader)
        
        return {"centroids": centroids, "clusterId": clusterId}
    

    # Initializes centroids randomly from the set of input data.
    def init_centroids(self, df, k):
        return df.sample(n=k, random_state=1, ignore_index=True).values.tolist()


    # preprocess removes all empty lists from the list of centroids, because they are not useful.
    def preprocess(self, centroids, df, k):
        count = 0
        for i in range(len(centroids)):
            if centroids[i] == []:
                count += 1

        for i in range(count):
            centroids.remove([])
    
        return centroids
