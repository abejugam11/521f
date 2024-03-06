import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Assignment03 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder.appName("Assignment03").master("local").getOrCreate()

    try {
      // Hardcoded file path (for testing purposes)
      val inputPath = "../departuredelays.csv"
      //val inputPath = args(0)

      // Read data from the hardcoded path
      val flightsDF = spark.read.csv(inputPath).toDF("distance", "origin", "destination", "date", "delay")

      // Query 1
      val resultQuery1 = flightsDF.filter(col("distance") > 1000).select("distance", "origin", "destination").orderBy(desc("distance")).limit(10)
      println("Query 1 Result:")
      resultQuery1.show()

      // Query 2
      val resultQuery2 = flightsDF.filter((col("delay") > 120) && (col("origin") === "SFO") && (col("destination") === "ORD")).select("date", "delay", "origin", "destination").orderBy(desc("delay")).limit(10)
      println("Query 2 Result:")
      resultQuery2.show()

    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
