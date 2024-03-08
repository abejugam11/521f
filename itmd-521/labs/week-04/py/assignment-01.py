from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when, count


spark = SparkSession.builder.appName("Assignment01").getOrCreate()

csv_file_path = "../Divvy_Trips_2015-Q1.csv"


data_set = spark.read.option("header", "true").csv(csv_file_path)
print("DataFrame 1:")
data_set.printSchema()
print("Record Count: " + str(data_set.count()))


schema = StructType([
    StructField("trip_id", IntegerType(), False),
    StructField("start_time", StringType(), False),

])

data_set2 = spark.read.schema(schema).option("header", "true").csv(csv_file_path)
print("\nDataFrame 2:")
data_set2.printSchema()
print("Record Count: " + str(data_set2.count()))


Schema_ddl = "trip_id INT, start_time STRING, ... "

data_set3 = spark.read.option("header", "true").option("inferSchema", "false").schema(Schema_ddl).csv(csv_file_path)
print("\nDataFrame 3:")
data_set3.printSchema()
print("Record Count: " + str(data_set3.count()))

data_set_filtered = data_set3.select("Gender", "Last Name") \
    .filter(
        (col("Last Name").between("A", "K") & (col("Gender") == "female")) |
        (col("Last Name").between("L", "Z") & (col("Gender") == "male"))
    )

grouped_df = data_set_filtered.groupBy("station").agg(count("*").alias("Total"))
grouped_df.show(10)


spark.stop()
