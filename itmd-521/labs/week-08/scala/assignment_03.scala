import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object Assignment03 {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("Assignment03")
      .config("spark.master", "local[*]")  // Use "local[*]" for local mode
      .getOrCreate()

     import spark.implicits._
    try {
      // Part 1: Reading and Querying CSV
      val inputPath = args(0)
      val flightsDF = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)

      // Query 1
      val resultQuery1 = flightsDF
        .filter(col("distance") > 1000)
        .select("distance", "origin", "destination")
        .orderBy(desc("distance"))
        .limit(10)
      println("Query 1 Result:")
      resultQuery1.show()

      // Query 2
      val resultQuery2 = flightsDF
        .filter((col("delay") > 120) && (col("origin") === "SFO") && (col("destination") === "ORD"))
        .select("date", "delay", "origin", "destination")
        .orderBy(desc("delay"))
        .limit(10)
      println("Query 2 Result:")
      resultQuery2.show()

      // Part 2: Filtering and Catalog
      val usDelayFlightsSchema = new StructType()
        .add(StructField("date", StringType, true))
        .add(StructField("delay", IntegerType, true))
        .add(StructField("distance", IntegerType, true))
        .add(StructField("origin", StringType, true))
        .add(StructField("destination", StringType, true))

      val usDelayFlightsDF = spark.read
        .format("csv")
        .option("header", "true")
        .schema(usDelayFlightsSchema)
        .load(inputPath)

      usDelayFlightsDF.createOrReplaceTempView("us_delay_flights_tbl")

      val chicagoFlightsDF = usDelayFlightsDF
        .filter(
          col("origin") === "ORD" &&
          substring(col("date"), 1, 2) === "03" &&
          substring(col("date"), 3, 2).between("01", "15")
        )

      chicagoFlightsDF.show(5)

      val catalogColumns = spark.catalog.listColumns("us_delay_flights_tbl")
      val columnNames = catalogColumns.map(_.name)

      println("Columns of us_delay_flights_tbl:")
      columnNames.foreach(name => println(name))

      // Part 3: Writing to Different Formats
      val departuredelaysDF: DataFrame = usDelayFlightsDF

      departuredelaysDF.write.mode("overwrite").json("departuredelays.json")
      departuredelaysDF.write.mode("overwrite").option("compression", "lz4").json("departuredelays_lz4.json")
      departuredelaysDF.write.mode("overwrite").parquet("departuredelays.parquet")

      // Part 4: Filtering and Writing ORD Records
      val ordDeparturesDF: DataFrame = spark.read.parquet("departuredelays.parquet")

      val formattedDeparturesDF: DataFrame = ordDeparturesDF
        .withColumn("date", to_timestamp(col("date"), "MMddHHmm"))
        .withColumn("formatted_date", date_format(col("date"), "MM-dd hh:mm a"))

      val filteredOrdDeparturesDF: DataFrame = formattedDeparturesDF
        .filter(col("origin") === "ORD")

      filteredOrdDeparturesDF.write.mode("overwrite").parquet("orddeparturedelays.parquet")

      filteredOrdDeparturesDF
        .select("formatted_date", "delay", "distance", "origin", "destination")
        .show(10, truncate = false)

    } catch {
      case e: Exception => println(s"An error occurred: ${e.getMessage}")
    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
