import time
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType
def say_hello(name:str)->str:
    return "hello {}".format(name)
cf = SparkConf().set("spark.executor.instances", "4").set("spark.executor.cores", "5")
say_hello_udf = udf(lambda name:say_hello(name))
sc = SparkContext(conf=cf)
spark = SparkSession(sc)
#("riemann",),("ramanujan",),("gauss",),("newton",)
# df = spark.createDataFrame([("madhu",),("albert",),("dirac",),("riemann",),("ramanujan",),("gauss",),("newton",)],["name"])
# ls = sc.parallelize([1,2,3,4,5])
# print(ls.collect())
# df2 = df.withColumn("greetings",say_hello_udf(col("name")))
# df2.show()
# print(df2.explain())
# print(df2.rdd.getNumPartitions())
fileRead = spark.read.csv("F:/data/student.txt")
print(fileRead.take(2))
time.sleep(1000)