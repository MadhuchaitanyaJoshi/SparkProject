from pyspark import SparkContext
from pyspark.sql import SparkSession,Row
import pyspark.sql.functions as f
from pyspark.sql.types import StructType,StructField,IntegerType,MapType,StringType
from pyspark.sql.functions import col
sc = SparkContext()
spark = SparkSession(sc)

schema = StructType([StructField("id",IntegerType(),True),StructField("value",MapType(StringType(),StringType()),True)])
file = spark.read.csv("F:/data/jio.txt",sep="|",header=True,inferSchema=True)
print(file.printSchema())
print(file.show())

print(file.show())
df2 = file.withColumn("a",f.explode(file.value))
print(df2.printSchema())
print(df2.show())