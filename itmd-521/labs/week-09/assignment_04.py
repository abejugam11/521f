from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,col, when, current_date


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Assignment 4") \
    .getOrCreate()
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
# Stop the SparkSession
spark.stop()
