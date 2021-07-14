from pyspark import SparkContext

sc=  SparkContext()

file = sc.textFile("F:/data/kpmg.txt")
lines = file.map(lambda x:(1,1)).reduceByKey(lambda x,y:x+y)
print("lines------------->",lines.collect())

wordcount = file.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
print("wordcount------------>",wordcount.collect())
