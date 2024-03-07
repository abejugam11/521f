from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Assignment03").getOrCreate()

    try:
        # Hardcoded file path (for testing purposes)
        #input_path = "../departuredelays.csv"
        input_path=sys.argv[1]
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
    
    
        # Create a Spark session
        spark = SparkSession.builder.appName("FlightAnalysis").config("spark.sql.catalogImplementation", "hive").getOrCreate()

        # Read the CSV file and create a DataFrame named us_delay_flights_df
        us_delay_flights_df = spark.read.csv(input_path, header=True, inferSchema=True)

        # Create a table named us_delay_flights_tbl
        us_delay_flights_df.createOrReplaceTempView("us_delay_flights_tbl")

        # Create a DataFrame for flights with an origin of Chicago (ORD) and a month/day combo between 03/01 and 03/15
        chicago_flights_df = us_delay_flights_df.filter((col("origin") == "ORD") & (col("date").between("03/01", "03/15")))

        # Show the first 5 records of the DataFrame
        chicago_flights_df.show(5)

        # Use Spark Catalog to list the columns of table us_delay_flights_tbl
        catalog_columns = spark.catalog.listColumns("us_delay_flights_tbl")
        column_names = [col.name for col in catalog_columns]

        print("Columns of us_delay_flights_tbl:")
        for column_name in column_names:
            print(column_name)
        
    finally:
        # Stop the Spark session
        spark.stop()
