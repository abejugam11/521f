from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,col, when, current_date


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Assignment 4") \
    .getOrCreate()

# Read data from the titles table where title is 'Senior Engineer'
senior_engineers_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
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
left_senior_engineers_df.createOrReplaceTempView("left_senior_engineers")

# Write DataFrame as an unmanaged table to the database
left_senior_engineers_df.write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("dbtable", "left_table_unmanaged") \
    .option("user", "root") \
    .option("password", "root") \
    .option("path", "/path/to/save/left_table_unmanaged") \
    .mode("overwrite") \
    .save()

# Write DataFrame tempView to the database
spark.sql("CREATE OR REPLACE TEMPORARY VIEW left_tempview AS SELECT * FROM left_senior_engineers")
spark.sql("CREATE OR REPLACE TEMPORARY VIEW left_df AS SELECT * FROM left_senior_engineers")

# Write DataFrame to the database, setting mode type to 'errorifexists' to generate an error
try:
    left_senior_engineers_df.write.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/employees") \
        .option("dbtable", "left_table_unmanaged") \
        .option("user", "root") \
        .option("password", "root") \
        .mode("errorifexists") \
        .save()
except Exception as e:
    print("Error:", e)
    
# Stop the SparkSession
spark.stop()
