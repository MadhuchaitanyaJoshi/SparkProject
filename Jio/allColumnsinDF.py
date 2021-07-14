from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import map_keys

sc = SparkContext()
spark = SparkSession(sc)


ds = sc.parallelize([
  (1, {"foo": (1, "a"), "bar": (2, "b")}),
  (2, {"foo": (3, "c")}),
  (3, {"bar": (4, "d")})]
)
df = spark.createDataFrame(ds,["id", "alpha"])
print(df.show(20,False))

