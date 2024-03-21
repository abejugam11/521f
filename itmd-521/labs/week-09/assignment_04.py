from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,col, when, current_date


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Assignment 4") \
    .getOrCreate()

# Read data from the titles table where title is 'Senior Engineer'
senior_engineers_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://172.17.0.2:3306/employees") \
    .option("user", "root") \
    .option("password", "root") \
    .option("query", "SELECT * FROM titles WHERE title = 'Senior Engineer'") \
    .load()

# Add a temp column to identify if the senior engineer is current or has left
senior_engineers_df = senior_engineers_df.withColumn("status",
                                                     when(col("to_date") == "9999-01-01", "current")
                                                     .otherwise(when(col("to_date") < current_date(), "left")))

# Count how many senior engineers have left and how many are current
left_count = senior_engineers_df.filter(col("status") == "left").count()
current_count = senior_engineers_df.filter(col("status") == "current").count()
print("Number of senior engineers who have left:", left_count)
print("Number of current senior engineers:", current_count)

# Create a PySpark SQL table of just the Senior Engineers information that have left the company
left_senior_engineers_df = senior_engineers_df.filter(col("status") == "left")

left_senior_engineers_df.write.format("jdbc") \
    .option("url", "jdbc:mysql://172.17.0.2:3306/employees") \
    .option("dbtable", "left_table_unmanaged") \
    .option("user", "root") \
    .option("password", "root") \
    .mode("overwrite") \
    .save()

# Create PySpark SQL tempView of senior engineers who have left the company
left_senior_engineers_df.createOrReplaceTempView("left_tempview")

# Create PySpark DataFrame of senior engineers who have left the company
left_df = left_senior_engineers_df

# Show the DataFrame
left_df.show(10)

# Query the temporary view
result = spark.sql("SELECT * FROM left_tempview")
result.show(10)

# Write DataFrame to the database, setting mode type to 'errorifexists' to generate an error
try:
    left_senior_engineers_df.write.format("jdbc") \
        .option("url", "jdbc:mysql://172.17.0.2:3306/employees") \
        .option("dbtable", "left_table_unmanaged") \
        .option("user", "root") \
        .option("password", "root") \
        .mode("errorifexists") \
        .save()
except Exception as e:
    print("Error:", e)
    
# Stop the SparkSession
spark.stop()
