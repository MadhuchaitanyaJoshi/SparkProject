from pyspark import SparkContext
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType,IntegerType,MapType,TimestampType
sc = SparkContext()
spark = SparkSession(sc)

a = sc.parallelize([("2011-08-12T20:17:46.384Z",)])
d = spark.createDataFrame(a).toDF("t")
#d = d.withColumn("t",to_date(to_utc_timestamp(date_format(d["t"], "yyyy-MM-dd'T'HH:mm:ss.SSSS"),"Europe/Berlin"),"mm-dd-yyyy"))
# d = d.withColumn("t",to_date(to_utc_timestamp(date_format(d["t"], "yyyy-MM-dd'T'HH:mm:ss.SSSS"),"Europe/Berlin"),"mm-dd-yyyy"))
# print(d.show(20,False))
#
# print(d.printSchema())

d2 = d.withColumn("tsFromString",to_timestamp(d["t"],"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
d3 = d2.withColumn("day_add",date_add(d2["t"],3))
print(d2.printSchema(),d2.show(20,False))
print(d3.printSchema(),d3.show(20,False))
d4 = d3.select(months_between(current_timestamp(),current_timestamp()))
print(d4.show(20,False))
#d4.filter(d4["t"])
# d5 = d.withColumn("doy",dayofweek(d["t"]))
# print(d5.show(20,False))