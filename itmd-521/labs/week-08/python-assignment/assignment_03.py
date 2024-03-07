from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, to_timestamp,date_format
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

try:
    # Create a Spark session
    spark = SparkSession.builder.appName("Assignment03").config("spark.sql.catalogImplementation", "hive").getOrCreate()

    # Part 1: Reading and Querying CSV
    input_path = sys.argv[1]
    flights_df = spark.read.csv(input_path, header=True, inferSchema=True)

    result_query1 = flights_df.filter(col("distance") > 1000).select("distance", "origin", "destination").orderBy("distance", ascending=False).limit(10)
    print("Query 1 Result:")
    result_query1.show()

    result_query2 = flights_df.filter((col("delay") > 120) & (col("origin") == 'SFO') & (col("destination") == 'ORD')).select("date", "delay", "origin", "destination").orderBy("delay", ascending=False).limit(10)
    print("Query 2 Result:")
    result_query2.show()

    # Part 2: Filtering and Catalog
    us_delay_flights_df = spark.read.csv(
        input_path,
        header=True,
        schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
    )
    us_delay_flights_df.createOrReplaceTempView("us_delay_flights_tbl")

    chicago_flights_df = us_delay_flights_df.filter(
        (col("origin") == "ORD") & (substring(col("date"), 1, 2) == "03") & (substring(col("date"), 3, 2).between("01", "15"))
    )
    chicago_flights_df.show(5)

    catalog_columns = spark.catalog.listColumns("us_delay_flights_tbl")
    column_names = [col.name for col in catalog_columns]

    print("Columns of us_delay_flights_tbl:")
    for column_name in column_names:
        print(column_name)

    # Part 3: Writing to Different Formats
    departuredelays_df = spark.read.csv(
        input_path,
        header=True,
        schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
    )
    departuredelays_df.write.mode("overwrite").json("departuredelays.json")
    departuredelays_df.write.mode("overwrite").option("compression", "lz4").json("departuredelays_lz4.json")
    departuredelays_df.write.mode("overwrite").parquet("departuredelays.parquet")

    # Part 4: Filtering and Writing ORD Records

    #departuredelays_df = departuredelays_df.withColumn("date", to_timestamp(col("date"), "MMddHHmm"))

    #ord_departures_df = departuredelays_df.filter(departuredelays_df["origin"] == "ORD")

    # Extract month, day, and time for display without the year
    #ord_departures_df = ord_departures_df.withColumn("month_day", substring(col("date"), 1, 3))
    #ord_departures_df = ord_departures_df.withColumn("time", substring(col("date"), 6, 4))

    # Drop the original date column
    #ord_departures_df = ord_departures_df.drop("date")

    # Reorder columns for better display
    #ord_departures_df = ord_departures_df.select("month_day", "time", "delay", "distance", "origin", "destination")

    #ord_departures_df.write.mode("overwrite").parquet("orddeparturedelays.parquet")
    #ord_departures_df.show(10, truncate=False)
    departuredelays_df = spark.read.parquet("departuredelays.parquet")

    # Convert the "date" column to a timestamp
    departuredelays_df = departuredelays_df.withColumn("date", to_timestamp(col("date"), "MMddHHmm"))

    # Format the timestamp to "MM-dd hh:mm a"
    departuredelays_df = departuredelays_df.withColumn("formatted_date", date_format(col("date"), "MM-dd hh:mm a" ))

    # Select all records with ORD as the origin
    ord_departures_df = departuredelays_df.filter(departuredelays_df["origin"] == "ORD")

    # Write the results to a DataFrameWriter named orddeparturedelays in Parquet format
    ord_departures_df.write.mode("overwrite").parquet("orddeparturedelays.parquet")

    # Show the first 10 lines of the DataFrame
    ord_departures_df.select("formatted_date", "delay", "distance", "origin", "destination").show(10, truncate=False)


except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Stop the Spark session
    spark.stop()
