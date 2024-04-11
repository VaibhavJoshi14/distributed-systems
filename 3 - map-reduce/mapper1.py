mapperAddress = "localhost:50051"
selfId = 1
masterAddress = "localhost:50050" # remains same

from mapReduce_Kmeans import mapper

mapper1 = mapper.Mapper(mapperAddress, selfId=selfId, masterAddress=masterAddress)

mapper1.join()