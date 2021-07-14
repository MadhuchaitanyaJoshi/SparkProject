from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr
sc = SparkContext()
spark = SparkSession(sc)

a= spark.read.csv("F:/data/csvMultipleSmallFiles/*.csv",header=True,inferSchema=True)
b= spark.read.json("F:/data/csvMultipleSmallFiles/*.json")
# print(a.show(10,False))
# print(b.show(10,False))



print(a.select(col("id"),expr("name")).show())