import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object Assignment03 {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder.appName("Assignment03").getOrCreate()

    try {
      // Part 2: Filtering and Catalog
      val inputPath = args(0)
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
      columnNames.foreach(println)

    } catch {
      case e: Exception => println(s"An error occurred: $e")
    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
