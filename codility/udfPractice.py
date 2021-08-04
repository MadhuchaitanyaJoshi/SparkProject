from pyspark import SparkContext
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType,IntegerType,MapType
sc = SparkContext()
spark = SparkSession(sc)
schema =StructType([StructField("id",StringType()),StructField("name",StringType()),StructField("salary",StringType())])
# df = spark.read.csv("F:/data/student.txt").toDF("id","name","salary")
# print(df.show(10))

student2  = sc.textFile("F:/data/student2.csv")
stInter = student2.map(lambda x:x.split("|")[0].split(","))
k = stInter.filter(lambda x:int(x[2])>100)
print(stInter.collect())
a = spark.createDataFrame(stInter,schema=schema)
a = a.withColumn("id",a["id"].cast(IntegerType())).withColumn("salary",a["salary"].cast(IntegerType()))
print(a.show())
print(a.printSchema())


def adder(k):
    return k+150

def sLen2(name,salary):
    print(3)
    return 1
adder = udf(adder,IntegerType())
sLen = udf(lambda name : len(name))
sLen2 = udf(sLen2,MapType)
a = a.withColumn("salarydd",adder("salary")).withColumn("nameLength",sLen("name"))
b=  a.withColumn("nameLength2",sLen2(a.name,a.salary))
print(a.show())
print(b.show())