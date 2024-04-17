import pandas as pd
import numpy as np
import grpc
from mapReduce_Kmeans import map_reduce_kmeans_pb2, map_reduce_kmeans_pb2_grpc
import threading
from concurrent import futures
import time
# Fault tolerance can be ensured by each of the mapper processes sending a heartbeat at 
# regular intervals to the master [Not done].

class Master(map_reduce_kmeans_pb2_grpc.MasterServicesServicer):
    def __init__(self, selfAddress, mapperAddresses, reducerAddresses):
        self.selfAddress = selfAddress
        self.mapperAddresses = mapperAddresses
        self.reducerAddresses = reducerAddresses
        self.numMappers = len(mapperAddresses)
        self.numReducers = len(reducerAddresses)

        # initialize the grpc server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        map_reduce_kmeans_pb2_grpc.add_MasterServicesServicer_to_server(self, self.server)
        
        port = self.selfAddress.split(":")[1]
        self.server.add_insecure_port("[::]:" + port)
        self.server.start()
        print("Server started, listening on " + port)
        self.mappersResponded = [0 for i in range(self.numMappers)]
        self.reducersResponded = [0 for i in range(self.numReducers)]
        self.newCentroids = [None] * self.numReducers

        self.dumpFile = 'dump.txt'
        with open(self.dumpFile, 'w') as f:
            f.write("\nDump file of master process\n" )


    # Inputs to the function kmeans:
    #    df: dataframe (pandas) to cluster.
    #    inputFile: path of file where df is stored.
    #    k: number of centroids (integer).
    #    maxIter: maximum iterations to run the algorithm for (integer).
    #    maxAttempts: maximum retries to make if the clusters returned are less than k (integer).
    def kmeans(self, df, inputFile, k, maxIter=100, maxAttempts=100, dfHasHeader=True):
        # Step 1: Randomly initialize k cluster centroids.
        centroids = self.init_centroids(df, k)

        _centroids = []
        max_iter = maxIter

        # Produce splits, according to the number of mappers
        splits = self.generateSplits(df)
        print("Splits on data of size ", len(df), "for numMappers ", self.numMappers, " are ", splits)
        
        # Step 2: Repeat until the centroids converge or the max iter limit has reached.
        while True:
            with open(self.dumpFile, 'a') as f:
                f.write("-------------------------------------------\n")
                f.write("Iteration:" + str(maxIter - max_iter) + "\n")
                f.write("Current centroids" + centroids.__str__()+ "\n")
                
                print("-------------------------------------------")
                print("Iteration:", maxIter - max_iter)
                print("Current centroids", centroids)

            # Send the Map rpc to each mapper to work on their splits.
            for i in range(self.numMappers):
                request = map_reduce_kmeans_pb2.MapRequest(
                    inputStartIndex = splits[i][0],
                    inputEndIndex = splits[i][1],
                    inputFile = inputFile,
                    dfHasHeader = dfHasHeader,
                    centroids = self.getDataCentroids(centroids),
                    numReducers = self.numReducers
                )

                with grpc.insecure_channel(self.mapperAddresses[i]) as channel:
                    
                    with open(self.dumpFile, 'a') as f:
                        f.write("Sending Map request to mapper " + str(i+1) + ".\n")
                        print("Sending Map request to mapper " + str(i+1) + ".")

                    stub = map_reduce_kmeans_pb2_grpc.MapperStub(channel)
                    # it just sends the request, and the Mapper immediately
                    # replies with Ok, and the mapper starts the job. The 
                    # job is thus done parallely by each mapper, which reply
                    # later after completion to the master.
                    response = stub.Map(request)

            # Wait till all the mappers have completed.
            while(sum(self.mappersResponded) < self.numMappers):
                time.sleep(0.01)
            # Reset these counters. For use in next iteration.
            for i in range(self.numMappers):
                self.mappersResponded[i] = 0


            # After all mappers have returned to master successfully, master invokes 
            # reducers with necessary parameters.
            for i in range(self.numReducers):
                with grpc.insecure_channel(self.reducerAddresses[i]) as channel:
                    with open(self.dumpFile, 'a') as f:
                        f.write("Sending ReduceInit request to reducer " + str(i+1) + ".\n")
                        print("Sending ReduceInit request to reducer " + str(i+1) + ".")
                    stub = map_reduce_kmeans_pb2_grpc.ReducerStub(channel)
                    response = stub.ReduceInit(map_reduce_kmeans_pb2.Empty(id=0))

            # Wait till all the Reducers have completed.
            while(sum(self.reducersResponded) < self.numReducers):
                time.sleep(0.01)
            # Reset these counters. For use in next iteration.
            for i in range(self.numReducers):
                self.reducersResponded[i] = 0

            """# Step 2.1: Assign each point of the data to the nearest centroid.  
            for index, dataPoint in df.iterrows():
                clusterId[index] = assign_nearest_centroid(list(dataPoint), centroids)

            # Step 2.2: Recompute the centroid of each cluster
            _centroids = compute_cluster_centroids(df, clusterId, k)"""
            # update new centroid values
            _centroids = self.newCentroids
            # 
            max_iter -= 1
            if (max_iter > 0 and centroids != _centroids) == False:
                break
            
            centroids = self.preprocess(_centroids.copy(), df, k)
            _centroids = []
            self.newCentroids = self.newCentroids = [None] * self.numReducers
            print("updated centroids to : ", centroids)

            # retry when lesser clusters are identified. This depends on the random initialization.
            if len(centroids) < k and maxAttempts > 0:
                return self.kmeans(df, inputFile, k, maxIter, maxAttempts-1, dfHasHeader)
            
            

        
        return {"centroids": centroids, "clusterId": clusterId}
    

    def generateSplits(self, df):
        part = int(len(df) / self.numMappers)
        splits = [[part * i, part * (i+1) - 1] for i in range(self.numMappers - 1)]
        splits.append([part * (self.numMappers - 1), len(df) - 1])
        return splits

    # Used for converting centroids to the grpc message format Data.
    def getDataCentroids(self, centroids):
        _centroids_ = []
        for i in range(len(centroids)):
            _centroids_.append(map_reduce_kmeans_pb2.Data(data=centroids[i]))
        return _centroids_
    
    
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
    

    def MapResponse(self, request, context):
        self.mappersResponded[request.id - 1] = 1
        with open(self.dumpFile, 'a') as f:
            f.write("Mapper " + str(request.id) + " completed its job (SUCCESS).\n")
            print("Mapper", request.id, "completed its job (SUCCESS).")
        return map_reduce_kmeans_pb2.Reply(message="Ok")


    def ReduceResponse(self, request, context):
        self.reducersResponded[request.id - 1] = 1
        with open(self.dumpFile, 'a') as f:
            f.write("Reducer " + str(request.id) + " completed its job (SUCCESS).\n")
            print("Reducer", request.id, "completed its job (SUCCESS).")

        # Process the received list
        for item in request.data:
            print("Received data:", item.key, item.data)
            self.newCentroids[request.id - 1 ] = item.data 
            # Process the data as needed

        # Return a response
        return map_reduce_kmeans_pb2.Reply(message="Master received Data successfully")
