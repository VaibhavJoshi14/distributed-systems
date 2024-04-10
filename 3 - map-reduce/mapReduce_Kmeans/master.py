import pandas as pd
import numpy as np
import grpc
from mapReduce_Kmeans import map_reduce_kmeans_pb2, map_reduce_kmeans_pb2_grpc
import threading

masterAddress = "localhost:50050"
mapperAddresses = ["localhost:50051", "localhost:50052", "localhost:50053"]
reducerAddresses = ["localhost:50054", "localhost:50055"]
# This is the input data to perform k-means clustering on..
inputFile = "Input/points.txt"
numCentroids = 2
maxIter = 100

class Master:
    def __init__(self, address, mapperAddresses, reducerAddresses):
        self.address = address
        self.mapperAddresses = mapperAddresses
        self.reducerAddresses = reducerAddresses
        self.numMappers = len(mapperAddresses)
        self.numReducers = len(reducerAddresses)
        # no grpc server is required for master.

    # Inputs to the function kmeans:
    #    df: dataframe (pandas) to cluster.
    #    k: number of centroids (integer).
    #    maxIter: maximum iterations to run the algorithm for (integer).
    #    maxAttempts: maximum retries to make if the clusters returned are less than k (integer).
    def kmeans(self, df, k, maxIter=100, maxAttempts=100):
        # Step 1: Randomly initialize k cluster centroids.
        centroids = self.init_centroids(df, k)

        # will contain the clusterId assigned to each point.
        clusterId = [0 for i in range(len(df))]

        _centroids = []
        max_iter = maxIter
        # Step 2: Repeat until the centroids converge or the max iter limit has reached.
        while True:
            
            
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
                return kmeans(df, k, maxIter, maxAttempts-1)
        
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
