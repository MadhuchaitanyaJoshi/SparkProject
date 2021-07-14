from pyspark import SparkContext
sc = SparkContext()
a = []
studentRdd = sc.textFile("F:\\data\\student.txt")
print(studentRdd.take(3))

