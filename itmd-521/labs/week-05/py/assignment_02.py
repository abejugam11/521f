from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import month, year, count, col, when,weekofyear,avg , to_date
import sys




def main(input_path):
    # Initialize Spark session
    spark = SparkSession.builder.appName("assignment_02").getOrCreate()

    try:
        
        data_set = spark.read.csv(input_path, header=True)


        #What were all the different types of fire calls in 2018?

        data_set = data_set.withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy"))
        fire_calls_2018=data_set.filter(year("CallDate") == 2018)
        distinct_fire_call_types = fire_calls_2018.select('CallType').distinct()
        print('The Distinct Fire Calls in 2018 are :')
        distinct_fire_call_types.show(truncate=False)



        #What months within the year 2018 saw the highest number of fire calls?

        Highest_fireCalls_2018=data_set.filter(year("CallDate") == 2018)
        Highest_fireCalls_2018.groupBy(month("CallDate").alias("Month")).agg(count("*").alias("Total_FireCalls")).orderBy(col("Total_FireCalls").desc()).show(10)


        #Which neighborhood in San Francisco generated the most fire calls in 2018?

        Neighbour_Sanfrancisco_fireCalls=data_set.filter((year("CallDate") == 2018) & (col("City") == "SF"))
        Neighbour_Sanfrancisco_fireCalls.groupBy("Neighborhood").agg(count("*").alias("Total_FireCalls")).orderBy(col("Total_FireCalls").desc()).show(10)


        #Which neighborhoods had the worst response times to fire calls in 2018

        data_set_worstResponseTime = data_set.filter(year("CallDate") ==2018).withColumn("ResponseTime",col("Delay"))
        data_set_worstResponseTime.groupBy("Neighborhood").agg(avg("ResponseTime").alias("AvgResponseTime")).orderBy(col("AvgResponseTime").desc()).show(10, False)



        #Which week in the year in 2018 had the most fire calls?

        data_set_WeekFireCalls = data_set.filter(year("CallDate") == 2018)
        data_set_WeekFireCalls.groupBy(weekofyear("CallDate").alias("Week")).count().orderBy(col("count").desc()).withColumnRenamed("count", "Total_FireCalls").show(10, False)


        #Is there a correlation between neighborhood, zip code, and number of fire calls

        data_set_correlation = data_set.groupBy("Neighborhood", "Zipcode").agg(count("*").alias("Total_FireCalls")).orderBy(col("Total_FireCalls").desc())
        data_set_correlation.show(10)

        #How can we use Parquet files or SQL tables to store this data and read it back?

        data_set.write.parquet("fire_calls_data.parquet", mode="overwrite")
        parquet_Dataset = spark.read.parquet("fire_calls_data.parquet")
        parquet_Dataset.show(10)

        data_set.createOrReplaceTempView("fire_calls_table")
        data_set_SQL = spark.sql("SELECT * FROM fire_calls_table WHERE year(CallDateTS) = 2018")
        data_set_SQL.show(10)

    finally:
        # Stop the Spark session
        spark.stop()



if __name__ == "__main__":
    # Check if the correct number of command-line arguments is provided
    if len(sys.argv) != 2:
        print("Usage: spark-submit assignment_02.py /path/to/sf-fire-calls.csv")
        sys.exit(1)

    # Get the file path from the command-line arguments
    input_path = sys.argv[1]

    # Call the main function with the provided file path
    main(input_path)







