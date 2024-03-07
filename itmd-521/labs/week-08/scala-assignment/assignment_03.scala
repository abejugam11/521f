import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, substring, to_timestamp, date_format}
//import org.apache.spark.sql.SparkSession.implicits._

object Assignment03 {
  def main(args: Array[String]): Unit = {

     val spark = SparkSession.builder.appName("Assignment03").config("spark.sql.catalogImplementation", "hive").getOrCreate()
     import spark.implicits._
     try {
      // Create a Spark session
      //val spark = SparkSession.builder.appName("Assignment03").config("spark.sql.catalogImplementation", "hive").getOrCreate()

      // Part 1: Reading and Querying CSV
      val inputPath = args(0)
      val flightsDF = spark.read.csv(inputPath).toDF("date", "delay", "distance", "origin", "destination")

      val resultQuery1 = flightsDF.filter(col("distance") > 1000).select("distance", "origin", "destination").orderBy(col("distance").desc).limit(10)
      println("Query 1 Result:")
      resultQuery1.show()

      val resultQuery2 = flightsDF.filter((col("delay") > 120) && (col("origin") === "SFO") && (col("destination") === "ORD")).select("date", "delay", "origin", "destination").orderBy(col("delay").desc).limit(10)
      println("Query 2 Result:")
      resultQuery2.show()

      // Part 2: Filtering and Catalog
      val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
      val usDelayFlightsDF = spark.read.option("header", "true").schema(schema).csv(inputPath)
      usDelayFlightsDF.createOrReplaceTempView("us_delay_flights_tbl")

      val chicagoFlightsDF = usDelayFlightsDF.filter(
        (col("origin") === "ORD") && (substring(col("date"), 1, 2) === "03") && (substring(col("date"), 3, 2).between("01", "15"))
      )
      println("Chicago Flights:")
      chicagoFlightsDF.show(5)

      val catalogColumns = spark.catalog.listColumns("us_delay_flights_tbl")
      val columnNames = catalogColumns.map(_.name)

      println("Columns of us_delay_flights_tbl:")
      columnNames.foreach(println)

      // Part 3: Writing to Different Formats
      val departureDelaysDF = spark.read.option("header", "true").schema(schema).csv(inputPath)
      departureDelaysDF.write.mode("overwrite").json("departuredelays.json")
      departureDelaysDF.write.mode("overwrite").option("compression", "lz4").json("departuredelays_lz4.json")
      departureDelaysDF.write.mode("overwrite").parquet("departuredelays.parquet")

      // Part 4: Filtering and Writing ORD Records
      val ordDeparturesDF = spark.read.parquet("departuredelays.parquet")
        .withColumn("date", to_timestamp(col("date"), "MMddHHmm"))
        .withColumn("formatted_date", date_format(col("date"), "MM-dd hh:mm a"))

      val filteredORDDeparturesDF = ordDeparturesDF.filter(col("origin") === "ORD")
      filteredORDDeparturesDF.write.mode("overwrite").parquet("orddeparturedelays.parquet")

      println("ORD Departures:")
      filteredORDDeparturesDF.select("formatted_date", "delay", "distance", "origin", "destination").show(10, truncate = false)

    } catch {
      case e: Exception => println(s"An error occurred: ${e.getMessage}")
    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
