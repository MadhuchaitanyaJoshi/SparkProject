from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,map_keys
import json
sc = SparkContext()
spark = SparkSession(sc)

emp = spark.read.json("F:/data/nobel.json",multiLine=True).select(explode("prizes").alias("main"))
df1 = emp.withColumn("subject",emp["main"]["category"])
print(emp.show(20,False),emp.printSchema())
print(df1.show(20),False)
df2  =  emp.select(col("main.*"))
print(df2.show(20,False))

keysDF= df2.select(col("laureates").getItem(0))
keys = keysDF.rdd.keys().take(1)[0].__fields__

keysCol = [col(key) for key in keys]
print(keysCol)
# df2 = df2.select(col("laureates").getItem(0)["firstname"].alias("name"))
# print(df2.show(20,False))
df2.select()
