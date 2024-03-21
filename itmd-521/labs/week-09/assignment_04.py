from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,col, when, current_date


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Assignment 4") \
    .getOrCreate()

# Read the employees table into a DataFrame
employees_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://172.17.0.2:3306/employees") \
    .option("dbtable", "employees") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

# Display the count of the number of records in the DF
print("Number of records in employees_df:", employees_df.count())

# Display the schema of the Employees Table from the DF
employees_df.printSchema()

salaries_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://172.17.0.2:3306/employees") \
    .option("dbtable", "(SELECT * FROM salaries ORDER BY salary DESC LIMIT 10000) as top_salaries") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

# Write the DataFrame back to the database to a new table called aces
salaries_df.write.format("jdbc") \
    .option("url", "jdbc:mysql://172.17.0.2:3306/employees") \
    .option("dbtable", "aces") \
    .option("user", "root") \
    .option("password", "root") \
    .mode("overwrite") \
    .save()

# Write the DataFrame out to the local system as a CSV and save it to local system using snappy compression
salaries_df.write \
    .option("compression", "snappy") \
    .csv("output_folder")

# Read data from the titles table where title is 'Senior Engineer'
titles_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://172.17.0.2:3306/employees") \
    .option("user", "root") \
    .option("password", "root") \
    .option("query", "SELECT * FROM titles WHERE title = 'Senior Engineer'") \
    .load()

# Add temp column to identify if the senior engineer is current or has left
senior_engineers_df = titles_df.withColumn("status",
                                            when(col("to_date") == "9999-01-01", "current")
                                            .otherwise(when(col("to_date") < current_date(), "left")))

# Count how many senior engineers have left and how many are current
left_count = senior_engineers_df.filter(col("status") == "left").count()
current_count = senior_engineers_df.filter(col("status") == "current").count()
print("Number of senior engineers who have left:", left_count)
print("Number of current senior engineers:", current_count)

# Create a PySpark SQL table of senior engineers who have left the company
senior_engineers_left_df = senior_engineers_df.filter(col("status") == "left")
senior_engineers_left_df.createOrReplaceTempView("senior_engineers_left")

senior_engineers_left_df.write.format("jdbc") \
    .option("url", "jdbc:mysql://172.17.0.2:3306/employees") \
    .option("dbtable", "left_table_unmanaged") \
    .option("user", "root") \
    .option("password", "root") \
    .option("path", "../left_table_unmanaged") \
    .mode("overwrite") \
    .save()

senior_engineers_left_df.createOrReplaceTempView("left_tempview")

senior_engineers_left_df.write.format("jdbc") \
    .option("url", "jdbc:mysql://172.17.0.2:3306/employees") \
    .option("dbtable", "left_df") \
    .option("user", "root") \
    .option("password", "root") \
    .mode("overwrite") \
    .save()

# Error if table already exists
try:
    senior_engineers_left_df.write.format("jdbc") \
        .option("url", "jdbc:mysql://172.17.0.2:3306/employees") \
        .option("dbtable", "left_table") \
        .option("user", "root") \
        .option("password", "root") \
        .mode("errorifexists") \
        .save()
except Exception as e:
    print("Error:", e)


# Stop the SparkSession
spark.stop()
