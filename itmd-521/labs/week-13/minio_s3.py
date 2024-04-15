from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date

# Removing hard coded password - using os module to import them
import os
import sys

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

#conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
#conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
conf.set('spark.hadoop.fs.s3a.access.key', "spark521")
conf.set('spark.hadoop.fs.s3a.secret.key', "79a93eda-ba02-11ec-8a4c-54ee75516ff6")
conf.set("spark.hadoop.fs.s3a.endpoint", "http://10.0.0.50:9000")

#spark = SparkSession.builder.appName("JRH insert 30 to mysql").config('spark.driver.host','192.168.172.45').config(conf=conf).getOrCreate()
spark = SparkSession.builder.appName("abhi test").config('spark.driver.host','spark-edge-vm0.service.consul').config(conf=conf).getOrCreate()

df = spark.read.csv('s3a://itmd521/60.txt')
print('df', df)

