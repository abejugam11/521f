from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# Check if the correct number of command line arguments is provided
if len(sys.argv) != 2:
    print("Usage: spark-submit script.py <file_location>")
    sys.exit(1)

# Get the file location from the command line arguments
file_location = sys.argv[1]

# Create a Spark session
spark = SparkSession.builder.appName("Assignment03").getOrCreate()

# Read the input CSV file into a DataFrame
flights_df = spark.read.csv(file_location, header=True, inferSchema=True)

# Create a temporary SQL table
flights_df.createOrReplaceTempView("departuredelays")

# Query 1
query1_result = spark.sql("""
    SELECT distance, origin, destination 
    FROM departuredelays 
    WHERE distance > 1000 
    ORDER BY distance DESC
""").show(10)

# Query 2
query2_result = spark.sql("""
    SELECT date, delay, origin, destination 
    FROM departuredelays 
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
    ORDER by delay DESC
""").show(10)

# Stop the Spark session
spark.stop()
