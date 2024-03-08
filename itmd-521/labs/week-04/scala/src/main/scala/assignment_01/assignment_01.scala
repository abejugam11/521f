//package main.scala.assignment01
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

object Assignment01 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Assignment01").config("spark.master", "local").getOrCreate()

    val csvFilePath = "../Divvy_Trips_2015-Q1.csv"

    val data_set1 = spark.read.option("header", "true").csv(csvFilePath)
    println("DataFrame 1:")
    data_set1.printSchema()
    println("Record Count: " + data_set1.count())

    val schema = new StructType()
      .add(StructField("trip_id", IntegerType, true))
      .add(StructField("bike_id", IntegerType, true))
      .add(StructField("tripduration", StringType, true))
      .add(StructField("gender", StringType, true))
      .add(StructField("from_station_id", IntegerType, true))
      .add(StructField("from_station_name", StringType, true))
      .add(StructField("to_station_id", IntegerType, true))
      .add(StructField("to_station_name", StringType, true))
      .add(StructField("usertype", StringType, true))
      .add(StructField("birthyear", IntegerType, true))

    val data_set2 = spark.read.schema(schema).option("header", "true").csv(csvFilePath)
    println("\nDataFrame 2:")
    data_set2.printSchema()
    println("Record Count: " + data_set2.count())

    val Schema_ddl = "trip_id INT, bike_id INT, tripduration STRING, gender STRING, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, birthyear INT"

    val data_set3 = spark.read.option("header", "true").option("inferSchema", "false").schema(Schema_ddl).csv(csvFilePath)
    println("\nDataFrame 3:")
    data_set3.printSchema()
    println("Record Count: " + data_set3.count())

    

    val Filtered_data_set = data_set3.select("gender", "from_station_name").filter((data_set3("from_station_name").between("A", "K") && data_set3("gender") === "Female") || (data_set3("from_station_name").between("L", "Z") && data_set3("gender") === "Male"))
    val groupedDF = Filtered_data_set.groupBy("from_station_name").count()
    groupedDF.show(10)

    spark.stop()
  }
}
