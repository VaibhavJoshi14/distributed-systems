# Only one master process must be invoked.
from mapReduce_Kmeans import *

master = Master(masterAddress, mapperAddresses, reducerAddresses)

df = pd.read_csv(inputFile, header=None)
print("Input file--------")
print(df.head())
print("------------------")

ret = master.kmeans(df, numCentroids, maxIter=maxIter)
print(ret)