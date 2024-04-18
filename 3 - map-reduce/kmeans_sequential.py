# This file is not used in the distributed system. Just kept for reference.
import pandas as pd
import random
import numpy as np

# The algorithm clusters using k-means.
# df must only contain numeric data. Type checking for non-numeric data is not performed.
# k is the number of clusters of the data df that should be formed (Integer).
# This algorithm will become extremely slow to be usable for large datasets.
# max_attempts need not be changed, but can be increased if clustering is not able to find the required 
# number of clusters.

def kmeans_cluster(df, k, maxIter = 100, maxAttempts = 100):
    # Step 1: Randomly initialize k cluster centroids.
    centroids = init_centroids(df, k)
    # will contain the clusterId assigned to each point.
    clusterId = [0 for i in range(len(df))]

    _centroids = []
    max_iter = maxIter
    # Step 2: Repeat until the centroids converge or the max iter limit has reached.
    while True:
        print("current centroids", centroids)
        # Step 2.1: Assign each point of the data to the nearest centroid.  
        for index, dataPoint in df.iterrows():
            clusterId[index] = assign_nearest_centroid(list(dataPoint), centroids)
        print("current clusterids",  clusterId)
        # Step 2.2: Recompute the centroid of each cluster
        _centroids = compute_cluster_centroids(df, clusterId, k)
        
        max_iter -= 1
        if (max_iter > 0 and centroids != _centroids) == False:
            break

        centroids = preprocess(_centroids.copy(), df, k)
        
        # retry when lesser clusters are identified. This depends on the random initialization.
        if len(centroids) < k and maxAttempts > 0:
            return kmeans_cluster(df, k, maxIter, maxAttempts-1)
    
    return {"centroids": centroids, "clusterId": clusterId}


# preprocess removes all empty lists from the list of centroids, because they are not useful.
def preprocess(centroids, df, k):
    count = 0
    for i in range(len(centroids)):
        if centroids[i] == []:
            count += 1

    for i in range(count):
        centroids.remove([])
    
    return centroids


# Initializes centroids randomly from the set of input data.
def init_centroids(df, k):
    return df.sample(n=k, random_state=1, ignore_index=True).values.tolist()
    

# assign the nearest centroid to a data point from the list of centroids, using Euclidean distance.
def assign_nearest_centroid(dataPoint, centroids):
    nearest = 0
    nearest_distance = euclidean_distance(dataPoint, centroids[0])
    #print(centroids)
    for i in range(1, len(centroids)):
        if centroids[i] != []:
            dist = euclidean_distance(dataPoint, centroids[i])
            if dist < nearest_distance:
                nearest = i
                nearest_distance = dist
    return nearest


def euclidean_distance(point1, point2):
    return np.linalg.norm(np.array(point1) - np.array(point2))


def compute_cluster_centroids(df, clusterId, k):
    centroids = [[] for i in range(k)]
    for i in range(k):
        dataPoints = []
        for index, row in df.iterrows():
            if clusterId[index] == i:
                dataPoints.append(list(row))
        if dataPoints != []:
            print(i, dataPoints)
            centroids[i] = list(np.mean(dataPoints, axis=0))
        else:
            continue
    return centroids


if __name__ == "__main__":
    df = pd.read_csv('Input/points.txt', header=None)
    print("Input file--------")
    print(df.head())
    print("------------------")
    ret = kmeans_cluster(df=df, k=2)

    print("Clusters assigned ", ret['clusterId'])
    print("Centroids", ret['centroids'])
    import seaborn as sns
    import matplotlib.pyplot as plt
    from sklearn.manifold import TSNE
    data = pd.DataFrame()
    data["tsne0"] = df[0]
    data["tsne1"] = df[1]
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
    import pandas as pd
    df = pd.read_csv('Input/wdbc.data', header=None) # set k = 2 for this.
    df = pd.DataFrame(df.to_numpy()[:, 2:].astype('float64'))
    
    # read the seeds dataset
    df = pd.DataFrame(pd.read_csv('Input/seeds_dataset.txt', header=None, sep='\s+').to_numpy()[:, :-1])
    print(df.head())

    ret = kmeans_cluster(df=df, k=3)
    print("Clusters assigned ", ret['clusterId'])
    
    import seaborn as sns
    import matplotlib.pyplot as plt
    from sklearn.manifold import TSNE
    data = pd.DataFrame()
    tsne = TSNE(n_components=2, perplexity=50, n_iter=5000).fit_transform(df)
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
    plt.show()"""

