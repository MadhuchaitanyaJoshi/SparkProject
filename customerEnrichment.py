from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col
from datetime import datetime
import json
a=100
sc = SparkContext()
spark = SparkSession(sc)

with open("F:/Interview/revolve Solutions/customerJob/customer.json","r") as conf:
    conf = json.load(conf)
    print(conf)

start_timing = conf['start_time']
end_timing = conf['end_time']
dateStripFormat = conf['dateStripFormat']
transactionsPath = conf['transactionsPath']
customerPath = conf['customerPath']
productsPath = conf['productsPath']
def Enrichment(conf):
    try:
        start_time=datetime.strptime(start_timing,dateStripFormat )
        end_time=datetime.strptime(end_timing,dateStripFormat)
        transactions = spark.read.option('recursiveFileLookup','true').json(transactionsPath).drop("d")
        customer = spark.read.csv(customerPath,header=True)
        products = spark.read.csv(productsPath,header=True)
        transactions=transactions.withColumn("basket", explode("basket")).select("*", col("basket")["product_id"].alias("pid"), col("basket")["price"].alias("price")).drop("basket","price")
        transactions = transactions.join(customer,transactions['customer_id']==customer['customer_id'],'left').select(transactions['customer_id'],customer['loyalty_score'],transactions['date_of_purchase'],transactions['pid'])\
                .join(products,transactions['pid']==products['product_id'],'left').drop('pid','product_description')
        transactions = transactions.filter((col('date_of_purchase')>=start_time) & (col('date_of_purchase')<end_time)).groupBy("customer_id",'product_id','loyalty_score','product_category').count()

        print(transactions.orderBy('count',ascending=False).show(100,False))
        pandas_df = transactions.toPandas()
        print(pandas_df)
        return pandas_df
    except Exception as e:
        print("Caught exception:\n",e)
Enrichment(conf)