import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col,from_json,explode,map_keys
import fnmatch
import os
maindir = "F:/sparkOutputPath/"
filename = "loans_itr_alerts_1808.csv"
slash = "/"


sc = SparkContext()
spark = SparkSession(sc)

loan_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/practice") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "loans_itr_alerts_1808") \
    .option("user", "root").option("password", "root").load()
loan_df = loan_df.select("reactor_alert_context_json")

json_schema = spark.read.json(loan_df.rdd.map(lambda row: row.reactor_alert_context_json)).schema

dfmain = loan_df.withColumn('main', from_json(col('reactor_alert_context_json'), json_schema)).select("main")

keys = dfmain.rdd.keys().take(1)[0].__fields__
columnKeys = list(map(lambda f:col("main").getItem(f), keys))
final = dfmain.select(col("main").getItem("address"))
final_2 = dfmain.select(*columnKeys).toDF(*keys)
final_2.coalesce(1).write.csv(maindir+filename)
for file in os.listdir(maindir+filename):
    if fnmatch.fnmatch(file, '*.csv'):
        os.rename(maindir+filename+slash+file,maindir+filename+slash+filename)
        #you can use shututil.copyFile(SourcepathFull,DestinationpathFull)