from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

columnToBeChosen = "phone.p1"
a = spark.read.json("F:/data/jsonParsing/flat.json",multiLine=True)
print(a.show())
print(a.select(columnToBeChosen).show())
