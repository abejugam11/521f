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

# Writing the CSV data to 60-uncompressed folder
splitDF.write.format("csv").mode("overwrite").option("header","true").save("s3a://abejugam/60-uncompressed.csv")

# Writing the CSV data to 60-compressed folder with LZ4 compression
splitDF.write.format("csv").mode("overwrite").option("header","true").option("compression","lz4").save("s3a://abejugam/60-compressed.csv")

# Writing the DataFrame to Parquet format
splitDF.write.format("parquet").mode("overwrite").option("header","true").save("s3a://abejugam/60.parquet")

# Reading the uncompressed CSV data
writeDF=spark.read.csv('s3a://abejugam/60-uncompressed.csv')

# Coalescing DataFrame to a single partition for efficient writing
colesce_df = writeDF.coalesce(1)

# Writing the coalesced DataFrame to a single CSV file
colesce_df.write.format("csv").mode("overwrite").option("header","true").save("s3a://abejugam/60.csv")

# Convert the 'ObservationDate' column to a date type
writeDF = writeDF.withColumn("ObservationDate", writeDF["ObservationDate"].cast("Date"))

# Extract the year and month from the 'ObservationDate' column
writeDF = writeDF.withColumn("Year", year(writeDF["ObservationDate"]))
writeDF = writeDF.withColumn("Month", month(writeDF["ObservationDate"]))

# Calculate the average temperature per month per year
average_temp_df = writeDF.groupBy("Year", "Month").agg(avg("AirTemperature").alias("AverageTemperature"))

# Write the results to a Parquet file
average_temp_df.write.parquet("s3a://abejugam/part-three.parquet", mode="overwrite")

# Take only the first year's data (12 records) and write this to a CSV file
first_year_df = average_temp_df.filter(writeDF["Year"] == 1961).limit(12)
first_year_df.write.csv("s3a://abejugam/part-three.csv", header=True, mode="overwrite")

spark.stop()
