from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Assignment03").getOrCreate()

    try:
        # Hardcoded file path (for testing purposes)
        input_path = "departuredelays.csv"

        # Read data from the hardcoded path
        flights_df = spark.read.csv(input_path, header=True, inferSchema=True)

        # Query 1
        result_query1 = flights_df.filter(col("distance") > 1000).select("distance", "origin", "destination").orderBy("distance", ascending=False).limit(10)
        print("Query 1 Result:")
        result_query1.show()

        # Query 2
        result_query2 = flights_df.filter((col("delay") > 120) & (col("origin") == 'SFO') & (col("destination") == 'ORD')).select("date", "delay", "origin", "destination").orderBy("delay", ascending=False).limit(10)
        print("Query 2 Result:")
        result_query2.show()

    finally:
        # Stop the Spark session
        spark.stop()
