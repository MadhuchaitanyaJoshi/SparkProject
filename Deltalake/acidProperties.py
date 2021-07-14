from pyspark import SparkContext
from pyspark.sql import SparkSession
import time
sc = SparkContext()
spark = SparkSession(sc)
#df = spark.range(50).repartition(1).write.mode("append").csv("F:/data/acidPropertiesSpark/")
df = spark.range(50).repartition(1).rdd.map(lambda x:x['id'] if x['id']>50 else time.sleep(10))
print(df.collect())
