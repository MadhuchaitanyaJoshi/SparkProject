from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.streaming import StreamingContext

sc = SparkContext()
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 1)
topicPartion = TopicAndPartition('madhuConsumer',0)
fromOffset = {topicPartion: 0}

lines = KafkaUtils.createDirectStream(ssc, ['madhuConsumer'],{"bootstrap.servers": 'localhost:9092'}, fromOffsets=fromOffset)

lines.pprint()

lines = lines.map(lambda row: "roger")
print("*****************")
lines.pprint()
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
ssc.start()
ssc.awaitTermination()