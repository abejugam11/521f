from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

try:
    # Create a Spark session
    spark = SparkSession.builder.appName("assignment_03").config("spark.sql.catalogImplementation", "hive").getOrCreate()

    # Read the CSV file into a DataFrame with the appropriate schema
    departuredelays_df = spark.read.csv(
        "departuredelays.csv",
        header=True,
        schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
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
