from pyspark.sql import SparkSession
from pyspark.sql.functions import weekofyear, year,col
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import to_date

# Create a Spark session
spark = SparkSession.builder.appName("assignment_02").getOrCreate()

# Read the dataset
data_set = spark.read.csv("sf-fire-calls.csv", header=True)

#What were all the different types of fire calls in 2018?

data_set = data_set.withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy"))
fire_calls_2018=data_set.filter(year("CallDate") == 2018)
distinct_fire_call_types = fire_calls_2018.select('CallType').distinct()
print('The Distinct Fire Calls in 2018 are :')
distinct_fire_call_types.show(truncate=False)

