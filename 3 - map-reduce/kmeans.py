import pandas as pd
import random
import numpy as np
# This algorithm will become extremely slow to be usable for large datasets.
# df must only contain numeric data. Type checking for non-numeric data is not performed.
# k is the number of clusters of the data df that should be formed (Integer).
# max_attempts need not be changed, but can be increased if clustering is not able to find the required 
# number of clusters.
def kmeans_cluster(df, k, max_attempts = 100):
    # Step 1: Randomly initialize k cluster centroids.
    centroids = init_centroids(df, k)
    # will contain the clusterId assigned to each point.
    clusterId = [0 for i in range(len(df))]

    _centroids = []
    max_iter = 100
    # Step 2: Repeat until the centroids converge or the max iter limit has reached.
    while True:
        # Step 2.1: Assign each point of the data to the nearest centroid.
        centroids = preprocess(centroids, df, k)    
        for index, dataPoint in df.iterrows():
            clusterId[index] = assign_nearest_centroid(list(dataPoint), centroids)

        # Step 2.2: Recompute the centroid of each cluster
        _centroids = compute_cluster_centroids(df, clusterId, k)
        
        # retry when lesser clusters are identified. This depends on the random initialization.
        if num_distinct_values(clusterId, k) < k and max_attempts > 0:
            return kmeans_cluster(df, k, max_attempts-1)

        max_iter -= 1
        if (max_iter > 0 and has_changed(centroids, _centroids) == True) == False:
            break
        centroids = _centroids.copy()
    
    
    return {"centroids": centroids, "clusterId": clusterId}


def num_distinct_values(lst, k):
    count = 0
    done = [0 for i in range(k)]
    for i in range(k):
        if i in lst and done[i] == 0:
            count += 1
            done[i] = 1
    return count


def preprocess(centroids, df, k):
    count = 0
    for i in range(len(centroids)):
        if centroids[i] == []:
            count += 1

    for i in range(count):
        centroids.remove([])
    if centroids == [] or centroids == [[]]:
        centroids = init_centroids(df, k)
    return centroids


# Initializes centroids randomly.
def init_centroids(df, k):
    max_values = list(df.max().items())
    min_values = list(df.min().items())
    while True:
        centroids = [[round(random.uniform(min_values[i][1], max_values[j][1]), 3) for i in range(len(max_values))] for j in range(k)]
        # if all the centroids are different, then return otherwise retry
        if any_equal(centroids) == False:
            break
    return centroids

def any_equal(centroids):
    for i in range(len(centroids)):
        for j in range(i+1, len(centroids)):
            if centroids[i] == centroids[j]:
                return True
    return False

# returns true if the centroids have changed.
def has_changed(centroids, _centroids):
    # will require changes when the centroids marginally change.
    if centroids == _centroids:
        return False
    return True


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
            centroids[i] = list(np.mean(dataPoints, axis=0))
        else:
            continue
    return centroids


if __name__ == "__main__":
    df = pd.read_csv('datasets/points.txt', header=None)
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
    df = pd.read_csv('datasets/wdbc.data', header=None) # set k = 2 for this.
    df = pd.DataFrame(df.to_numpy()[:, 2:].astype('float64'))

    # read the seeds dataset
    df = pd.DataFrame(pd.read_csv('datasets/seeds_dataset.txt', header=None, sep='\s+').to_numpy()[:, :-1])
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

