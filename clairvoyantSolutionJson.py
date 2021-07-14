from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,IntegerType
import time

def checker(id,subjects):
    print(id)
    print(len(subjects))
    return len(subjects)+id
sc = SparkContext()
spark = SparkSession(sc)
access_key = "AKIAZVYYRDYNZNE5GPRM"
secret_key = "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"
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
df1 = spark.read.json("s3a://clairv/nesteddata/*",multiLine=True)
print(df1.show(20,False))


checkrudf =  udf(checker,IntegerType())
df2 = df1.withColumn("counter",checkrudf(df1["subjects"],df1["id"]))
print(df2.show(10,False))