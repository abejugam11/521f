from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Assignment 4") \
    .getOrCreate()

# Read the employees table into a DataFrame
employees_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("dbtable", "employees") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

# Display the count of the number of records in the DF
print("Number of records in employees_df:", employees_df.count())

# Display the schema of the Employees Table from the DF
employees_df.printSchema()

# Create a DataFrame of the top 10,000 employee salaries (sort DESC) from the salaries table
salaries_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("dbtable", "(SELECT * FROM salaries ORDER BY salary DESC LIMIT 10000) as top_salaries") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

# Write the DataFrame back to database to a new table called: aces
salaries_df.write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("dbtable", "aces") \
    .option("user", "root") \
    .option("password", "root") \
    .mode("overwrite") \
    .save()

# Write the DataFrame out to the local system as a CSV and save it to local system using snappy compression
salaries_df.write \
    .option("compression", "snappy") \
    .csv("output_folder")

# Stop the SparkSession
spark.stop()
