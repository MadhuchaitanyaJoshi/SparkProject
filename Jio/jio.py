from pyspark import SparkContext
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import explode
import json
ls= []
sc = SparkContext()
spark = SparkSession(sc)

file = spark.read.csv("F:/data/jio.txt",sep="|",header=True)
print(file.show())
print(file.printSchema())
p=file.rdd.collect()
for i in p:
    for value in json.loads(i[1]).values():
        ls.append(tuple(Row(i[0],value)))

print(ls)
s=spark.createDataFrame(sc.parallelize(ls),["id","name"])
print(s.show())

  