from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
import shutil
from glob import glob
import pyspark.streaming
import boto3
from botocore.exceptions import NoCredentialsError

sc = SparkContext()
spark = SparkSession(sc)
access_key = "AKIAJKJO7II4PFWCBWSQ"
secret_key = "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"

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

rdd = sc.parallelize([(1,"madhu")])
schema = ["id","name"]
df2 = spark.createDataFrame(rdd,schema)
df2.write.csv("s3://madhuchaitanya/file1.csv")

