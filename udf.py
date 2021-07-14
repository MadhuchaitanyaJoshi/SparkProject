from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType
def say_hello(name:str)->str:
    return "hello {}".format(name)

say_hello_udf = udf(lambda name:say_hello(name))
sc = SparkContext()
spark = SparkSession(sc)

df = spark.createDataFrame([("madhu",),("albert",),("dirac",),("riemann",),("ramanujan",),("gauss",),("newton",)],["name"])
print(df.show())
df2 = df.withColumn("greetings",say_hello_udf(col("name")))
print(df2.show())
