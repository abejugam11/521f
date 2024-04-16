from pyspark.sql import SparkSession

# Set the configuration properties
spark = SparkSession.builder.appName("MinIOTest").master("local[*]").getOrCreate()

s3accessKeyAws = "minioadmin"
s3secretKeyAws = "minioadmin"
connectionTimeOut = "600000"
s3endPointLoc = "http://127.0.0.1:9000"
sourceBucket = "minio-test-bucket"

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3endPointLoc)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3accessKeyAws)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3secretKeyAws)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", connectionTimeOut)

# Additional configurations
spark.sparkContext._jsc.hadoopConfiguration().set("spark.sql.debug.maxToStringFields", "100")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
