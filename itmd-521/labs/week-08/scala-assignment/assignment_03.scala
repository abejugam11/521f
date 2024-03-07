import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Assignment03 {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("Assignment03")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    try {
      // Part 4: Filtering and Writing ORD Records
      val departuredelays_df = spark.read.parquet("departuredelays.parquet")

      val formatted_df = departuredelays_df
        .withColumn("date", to_timestamp(col("date"), "MMddHHmm"))
        .withColumn("formatted_date", date_format(col("date"), "MM-dd hh:mm a"))

      val ord_departures_df = formatted_df.filter(col("origin") === "ORD")

      ord_departures_df.write.mode("overwrite").parquet("orddeparturedelays.parquet")

      ord_departures_df.select("formatted_date", "delay", "distance", "origin", "destination").show(10, truncate = false)
    } catch {
      case e: Exception => println(s"An error occurred: $e")
    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
