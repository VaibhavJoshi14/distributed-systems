# Only one master process must be invoked.
# This is the path of data to perform k-means clustering on..
inputFile = "Input/points.txt"
numCentroids = 2
maxIter = 100
dfHasHeader = False # determine whether the data has header or not. This must be correctly set.
sep = "," # separator in the data file.

# The below would require setting 3 reducer addresses in master.py here.
#inputFile = "Input/seeds_dataset.txt"
#numCentroids = 3
#maxIter = 100
#dfHasHeader = False
#sep = ","

# Addresses of master, mappers, and reducers. The size of the list of mapperAddresses specify 
# the number of mappers, and the size of the list of reducerAddresses specify the number of reducers.
# The number of reducers must be atleast the number numCentroids.
masterAddress = "localhost:50050"
mapperAddresses = ["localhost:50051", "localhost:50052", "localhost:50053"]
reducerAddresses = ["localhost:50054", "localhost:50055"]#, "localhost:50056"]


import pandas as pd
from mapReduce_Kmeans import master

master = master.Master(masterAddress, mapperAddresses, reducerAddresses)


if dfHasHeader == False:
    df = pd.read_csv(inputFile, header=None, sep=sep)
else:
    df = pd.read_csv(inputFile, sep=sep)

print("Input file--------")
print(df.head())
print("------------------")

ret = master.kmeans(df, inputFile, numCentroids, maxIter=maxIter, dfHasHeader=dfHasHeader, dfSep = sep)
print(ret)

"""
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE
data = pd.DataFrame()
tsne = TSNE(n_components=2, perplexity=10, n_iter=5000).fit_transform(df)
data["tsne0"] = tsne[:, 0]
data["tsne1"] = tsne[:, 1]
data["label"] = ret['clusterId']
sns.scatterplot(
    x="tsne0", y="tsne1",
    palette=sns.color_palette("hls", 5),
    hue="label",
    data=data,
    legend="full"
)
plt.show()
"""
