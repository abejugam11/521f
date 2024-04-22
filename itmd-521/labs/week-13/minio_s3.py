from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date,year, month, avg



# Removing hard coded password - using os module to import them
import os
import sys

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('SECRETKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('ACCESSKEY'))
# Configure these settings
# https://medium.com/@dineshvarma.guduru/reading-and-writing-data-from-to-minio-using-spark-8371aefa96d2
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# https://github.com/minio/training/blob/main/spark/taxi-data-writes.py
# https://spot.io/blog/improve-apache-spark-performance-with-the-s3-magic-committer/
conf.set('spark.hadoop.fs.s3a.committer.magic.enabled','true')
conf.set('spark.hadoop.fs.s3a.committer.name','magic')
# Internal IP for S3 cluster proxy
conf.set("spark.hadoop.fs.s3a.endpoint", "http://infra-minio-proxy-vm0.service.consul")

spark = SparkSession.builder.appName("abejugam convert 60.txt to csv").config('spark.driver.host','spark-edge.service.consul').config(conf=conf).getOrCreate()

df = spark.read.csv('s3a://itmd521/60.txt')

splitDF = df.withColumn('WeatherStation', df['_c0'].substr(5, 6)) \
.withColumn('WBAN', df['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(df['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', df['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', df['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', df['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', df['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', df['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', df['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', df['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', df['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', df['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', df['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', df['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', df['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', df['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', df['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', df['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', df['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

writeDF = splitDF.coalesce(1)

splitDF.write.format("csv").mode("overwrite").option("header","true").save("s3a://abejugam/60-uncompressed.csv")

splitDF.write.format("csv").mode("overwrite").option("header","true").option("compression","lz4").save("s3a://abejugam/60-compressed.csv")

splitDF.write.format("parquet").mode("overwrite").option("header","true").save("s3a://abejugam/60.parquet")

writeDF.write.format("csv").mode("overwrite").option("header","true").save("s3a://abejugam/60.csv")




# #part 2

csv_df=spark.read.csv('s3a://itmd521/60.txt')

# # Filter the data for the year 1961
df_1961 = csv_df.filter(year(csv_df['ObservationDate']) == 1961)

# # Extracting the month and year from the date column
df_1961 = df_1961.withColumn('month', month(df_1961['ObservationDate']))
df_1961 = df_1961.withColumn('year', year(df_1961['ObservationDate']))

# # Calculating the average temperature for each month in the year 1961
average_temp_df = df_1961.groupBy('year', 'month').agg(avg('AirTemperature').alias('average_temperature'))

# # Writing the results to a Parquet file
average_temp_df.write.format("parquet").mode("overwrite").option("header","true").save("s3a://abejugam/part-three.parquet")

# # Taking only the first year's data (12 records)
first_year_df = df_1961.limit(12)

# # Writing the first year's data to a CSV file
first_year_df.write.format("csv").mode("overwrite").option("header","true").save("s3a://abejugam/part-three.csv")

spark.stop()
