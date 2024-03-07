from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

try:
    # Create a Spark session
    spark = SparkSession.builder.appName("FlightDataProcessing").getOrCreate()

    # Define the schema for the DataFrame
    schema = StructType([
        StructField("date", DateType(), True),
        StructField("delay", IntegerType(), True),
        StructField("distance", IntegerType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
    ])

    # Read the CSV file into a DataFrame with the specified schema
    departuredelays_df = spark.read.csv(
        "../departuredelays.csv",
        header=True,
        schema=schema
    )

    # Write the content out as a JSON file
    departuredelays_df.write.mode("overwrite").json("departuredelays.json")

    # Write the content out as a compressed JSON file using lz4
    departuredelays_df.write.mode("overwrite").option("compression", "lz4").json("departuredelays_lz4.json")

    # Write the content out as a Parquet file
    departuredelays_df.write.mode("overwrite").parquet("departuredelays.parquet")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Stop the Spark session
    spark.stop()
