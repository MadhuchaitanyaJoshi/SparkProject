from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, MapType, StringType
import re
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when, split, lit, explode

sc = SparkContext()
spark = SparkSession(sc)

data = [({'ip': "10.34.24.2:1223", 'madhu': 2}, 1), ({'ip': "23.34.5.6"}, 2), ({'ip': "newserver", 'madhu': 3}, 3)]
df = spark.createDataFrame(data, ["labels", "name"])
print(df.show())
print("--------->",)
for i in df.head:
    print(i)
df2 = spark.read.csv("F:\\data\\serverDetails.csv", header=True)
df3 = df.withColumn("labels.ip", split(col("labels").getItem("ip"), ":")[0])
df3.rdd.collect()
print(df3.show())

keys_df = df.select(F.explode(F.map_keys(col("labels")))).distinct()
keys = list(map(lambda row: row[0], keys_df.collect()))
key_cols = list(map(lambda f: col("labels").getItem(f).alias(str(f)), keys))
final_cols = [col("name")] + key_cols
print(df.select(final_cols).show())
