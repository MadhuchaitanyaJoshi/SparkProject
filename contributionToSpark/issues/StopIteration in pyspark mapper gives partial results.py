from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster("local[2]")
sc = SparkContext(conf=conf)
def mapper(xx):
    if xx % 2 == 0:
        #raise StopIteration()
        pass
    else:
        return xx


out = sc.parallelize(range(100)).map(mapper).collect()
print(out)