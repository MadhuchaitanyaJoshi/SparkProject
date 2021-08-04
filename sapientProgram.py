from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
sc = SparkContext()
spark = SparkSession(sc)


click = spark.read.csv("F:/Interviews Septemer 2020/sapient/click.csv",header=True)
click = click.withColumn("ts",to_timestamp(click["tm"],"yyyy-MM-dd'T'HH:mm:SSS'Z'")).select("uid","tm","ts")
print(click.show(20,False),click.printSchema())
click = click.withColumn("tsNext",lead("ts").over(Window.partitionBy("uid").orderBy("ts")))
timeDiff = (col("tsNext").cast("long")-col("ts").cast("long"))/60
df2 = click.withColumn("diff",timeDiff)
part= Window.partitionBy(df2["uid"]).orderBy("ts").rangeBetween(Window.unboundedPreceding,0)
df2 = df2.withColumn("flag", when(df2["diff"]>30,1).otherwise(0))
df3 = df2.withColumn("summ",sum(df2["flag"]).over(part))
print(df2.show(20,False),df3.show(20,False))
df2.join