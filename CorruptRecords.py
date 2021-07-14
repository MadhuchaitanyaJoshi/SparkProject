from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
sconf = SparkConf().setAppName("CorruptChecker")
sc = SparkContext(conf=sconf)
spark = SparkSession(sc)

data = {"a":1,}

j = spark.read.json(sc.parallelize([data]),multiLine=True)
print(j.show())