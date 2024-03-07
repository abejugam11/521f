import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Assignment03 {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("Assignment03")
      .config("spark.master", "local[*]")
      .getOrCreate()

    try {
      // Part 3: Writing to Different Formats
      val inputPath = args(0)  // Assuming the input path is passed as a command-line argument

      val departuredelaysSchema = StructType(
        List(
          StructField("date", StringType, true),
          StructField("delay", IntegerType, true),
          StructField("distance", IntegerType, true),
          StructField("origin", StringType, true),
          StructField("destination", StringType, true)
        )
      )

      val departuredelaysDF: DataFrame = spark.read
        .schema(departuredelaysSchema)
        .csv(inputPath)

      // Write the content out as a JSON file
      departuredelaysDF.write.mode("overwrite").json("departuredelays.json")

      // Write the content out as a compressed JSON file using lz4
      departuredelaysDF.write.mode("overwrite").option("compression", "lz4").json("departuredelays_lz4.json")

      // Write the content out as a Parquet file
      departuredelaysDF.write.mode("overwrite").parquet("departuredelays.parquet")
    } catch {
      case e: Exception => println(s"An error occurred: $e")
    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
