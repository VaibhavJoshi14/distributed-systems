mapperAddress = "localhost:50051"

from mapReduce_Kmeans import mapper

mapper1 = mapper.Mapper(mapperAddress)

mapper1.join()