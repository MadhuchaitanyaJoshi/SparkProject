from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lag, lead, to_date

sc = SparkContext()
spark = SparkSession(sc)


sales = spark.read.csv("F:\data\steris\sales.csv",sep=" ",header=True,inferSchema=True)
sales = sales.withColumn("SALE_DATE",to_date(sales['SALE_DATE'],'yyyy-MM-dd'))
windowSale = Window.partitionBy("PRODUCT").orderBy("SALE_DATE")
sales= sales.withColumn("PREV_SALE_DATE",lag("SALE_DATE",1).over(windowSale)).withColumn("NEXT_SALE_DATE",lead("SALE_DATE",1).over(windowSale))
print(sales.show())
