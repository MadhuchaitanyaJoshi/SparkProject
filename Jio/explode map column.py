from pyspark import SparkContext
from pyspark.sql import SparkSession,Row
import pyspark.sql.functions as f
from pyspark.sql.types import StructType,StructField,IntegerType,MapType,StringType
import re
from pyspark.sql.functions import col,when,split
pattern = re.compile("[A-Za-z]+")

sc = SparkContext()
spark = SparkSession(sc)

data = [({'ip':"10.34.24.2:1223" , 'madhu': 2},1), ({'ip':"23.34.5.6"},2), ({'ip': "newserver", 'madhu': 3},3)]
df = spark.createDataFrame(data, ["labels","name"])
df2 = spark.read.csv("F:\\data\\serverDetails.csv",header=True)
print(df.show(10,False))
print(df2.show(10,False))

df3 = df.withColumn("new_col",when(pattern.fullmatch(col("labels.ip") is None),"no ip found"))
print(df3.show(10,False))

