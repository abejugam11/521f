from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
try:
    # Create a Spark session
    spark = SparkSession.builder.appName("ORDDepartureDelays").getOrCreate()
    
    schema = StructType([
        StructField("date", DateType(), True),
        StructField("delay", IntegerType(), True),
        StructField("distance", IntegerType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
    ])

    # Read the CSV file into a DataFrame with the specified schema
    departuredelays_df1 = spark.read.csv(
        "../departuredelays.csv",
        header=True,
        schema=schema
    )

    departuredelays_df1.write.mode("overwrite").parquet("departuredelays.parquet")
    # Read the Parquet file into a DataFrame
    departuredelays_df = spark.read.parquet("departuredelays.parquet")

    # Select all records with ORD as the origin
    ord_departures_df = departuredelays_df.filter(departuredelays_df["origin"] == "ORD")

    # Write the results to a DataFrameWriter named orddeparturedelays in Parquet format
    ord_departures_df.write.mode("overwrite").parquet("orddeparturedelays.parquet")

    # Show the first 10 lines of the DataFrame
    ord_departures_df.show(10)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Stop the Spark session
    spark.stop()
