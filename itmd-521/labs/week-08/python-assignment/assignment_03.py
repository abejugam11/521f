from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

try:
    # Create a Spark session
    spark = SparkSession.builder.appName("FlightAnalysis").config("spark.sql.catalogImplementation", "hive").getOrCreate()

    # Read the CSV file into a DataFrame with the appropriate schema
    us_delay_flights_df = spark.read.csv(
        "departuredelays.csv",
        header=True,
        schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
    )

    # Create a table named us_delay_flights_tbl
    us_delay_flights_df.createOrReplaceTempView("us_delay_flights_tbl")

    # Create a DataFrame for flights with an origin of Chicago (ORD) and a month/day combo between 03/01 and 03/15
    chicago_flights_df = us_delay_flights_df.filter(
        (col("origin") == "ORD") & (substring(col("date"), 1, 2) == "03") & (substring(col("date"), 3, 2).between("01", "15"))
    )

    # Show the first 5 records of the DataFrame
    chicago_flights_df.show(5)

    # Use Spark Catalog to list the columns of table us_delay_flights_tbl
    catalog_columns = spark.catalog.listColumns("us_delay_flights_tbl")
    column_names = [col.name for col in catalog_columns]

    print("Columns of us_delay_flights_tbl:")
    for column_name in column_names:
        print(column_name)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Stop the Spark session
    spark.stop()
