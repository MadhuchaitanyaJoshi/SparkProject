from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
# data = sc.parallelize([1,2,3,4,5,6,7,8])
# data2 = data.map(lambda x:x*x)

mango = sc.textFile("F:/data/mango.txt")
print(mango.collect())

mg = mango.flatMap(lambda x: str(x).split(" ")).map(lambda x:(x,1)).reduce(lambda x,y:x+y)
print()

