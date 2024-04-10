masterAddress = "localhost:50050"
mapperAddresses = ["localhost:50051", "localhost:50052", "localhost:50053"]
reducerAddresses = ["localhost:50054", "localhost:50055"]
# This is the input data to cluster.
inputFile = "Input/points.txt"

class Master:
    def __init__(self, address, mapperAddresses, reducerAddresses):
        self.address = address
        self.mapperAddresses = mapperAddresses
        self.reducerAddresses = reducerAddresses
        self.numMappers = len(mapperAddresses)
        self.numReducers = len(reducerAddresses)
        

    def kmeans(self, inputFile):



if __name__ == '__main__':
    master = Master(masterAddress, mapperAddresses, reducerAddresses)
    master.kmeans(inputFile)