import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, substring, to_timestamp, date_format}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object DepartureDelaysScala {
  def main(args: Array[String]): Unit = {
    // Create a Spark session with master configuration
    val spark = SparkSession.builder
      .appName("DepartureDelaysScala")
      .config("spark.master", "local[*]")  // Use "local[*]" for local mode
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    try {
      // Part 1: Reading and Writing CSV
      val inputPath = args(0)
      val departuredelaysDF: DataFrame = spark.read
        .option("header", "true")
        .schema(StructType(
          List(
            StructField("date", StringType, nullable = true),
            StructField("delay", IntegerType, nullable = true),
            StructField("distance", IntegerType, nullable = true),
            StructField("origin", StringType, nullable = true),
            StructField("destination", StringType, nullable = true)
          )
        ))
        .csv(inputPath)

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
