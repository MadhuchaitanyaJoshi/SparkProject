from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import time

sconf = SparkConf().setAppName("CorruptChecker")
sc = SparkContext(conf=sconf)
spark = SparkSession(sc)

rdd = [
    (1, 5),
    (2, 7),
    (3, 2)
]

rdd2 = [
    (1, 5),
    (2, 7),
    (1, 11),
    (2, 13),
    (3, 2)
]
rdd = sc.parallelize(rdd)
df = spark.createDataFrame(rdd,['id','marks'])

rdd2 = sc.parallelize(rdd2)
df2 = spark.createDataFrame(rdd2,['id','marks'])
print()
time.sleep(1000000)
