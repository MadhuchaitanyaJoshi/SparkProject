from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext()
spark = SparkSession(sc)

def cacheMethod(dataF):
    dataF.cache()
df = spark.read.csv("F:/data/developer_survey_2019/survey_results_public.csv")
cacheMethod(dataF=df)
print("count is:=====",df.count(),df.rdd.id())
df = df.filter(df["_c0"]<100)
cacheMethod(dataF=df)
print(df.count(),df.rdd.id())