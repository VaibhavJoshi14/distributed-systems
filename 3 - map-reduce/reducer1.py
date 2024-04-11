selfId = 1
selfAddress = "localhost:50054"
masterAddress = "localhost:50050"
mapperAddresses = ["localhost:50051", "localhost:50052", "localhost:50053"]

from mapReduce_Kmeans import reducer

red = reducer.Reducer(selfAddress=selfAddress, selfId=selfId, masterAddress=masterAddress, mapperAddresses=mapperAddresses)
red.join()