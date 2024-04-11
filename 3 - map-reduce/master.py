# Only one master process must be invoked.
# This is the path of data to perform k-means clustering on..
inputFile = "Input/points.txt"
numCentroids = 2
maxIter = 100
dfHasHeader = False # determine whether the data has header or not. This must be correctly set.

# Addresses of master, mappers, and reducers. The size of the list of mapperAddresses specify 
# the number of mappers, and the size of the list of reducerAddresses specify the number of reducers.
masterAddress = "localhost:50050"
mapperAddresses = ["localhost:50051",] #"localhost:50052", "localhost:50053"]
reducerAddresses = ["localhost:50054", "localhost:50055"]


import pandas as pd
from mapReduce_Kmeans import master

master = master.Master(masterAddress, mapperAddresses, reducerAddresses)


if dfHasHeader == False:
    df = pd.read_csv(inputFile, header=None)
else:
    df = pd.read_csv(inputFile)

print("Input file--------")
print(df.head())
print("------------------")

ret = master.kmeans(df, inputFile, numCentroids, maxIter=maxIter, dfHasHeader=dfHasHeader)
print(ret)
