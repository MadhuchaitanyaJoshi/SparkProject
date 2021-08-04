from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()
data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df=spark.createDataFrame(data,["id","input"])
print(df.show())
currentdate = df.select(current_date().alias("cur_date"),current_timestamp().alias("cur_timestamp"))
print(currentdate.show(20,False))
df_cur_date_change = currentdate.withColumn("cur_date_change",to_date(date_format(currentdate["cur_date"],"dd/MM/yyyy"),"dd/MM/yyyy"))
print(df_cur_date_change.printSchema(),df_cur_date_change.show())
df_cur_date_change = df_cur_date_change.withColumn("cur_date_change_2",date_format(df_cur_date_change["cur_date_change"],"yyyy/dd/MM"))
print(df_cur_date_change.show())
d5 = df.select(col("input"),months_between(current_date(),col("input")).alias("months_between"))
print(d5.show(20,False))