from pyspark import SparkContext
from pyspark import SparkConf

x=1
sc = SparkContext()
print(sc._jsc.sc().getExecutorMemoryStatus())
print(sc.getConf().getAll())


file2 = sc.textFile("F:/data/developer_survey_2019/survey_results_schema.csv")
print("num partitions:",file2.getNumPartitions())

arrRead = sc.parallelize([12],1)
print("num of partitions",arrRead.getNumPartitions())

file = sc.textFile("F:/data/developer_survey_2019/survey_results_public.csv")
print("num partitions:",file.getNumPartitions())


file3 = sc.textFile("F:/data/developer_survey_2019/survey_results_public_large.csv")
print("num partitions",file3.getNumPartitions())


print(file.toDebugString())