import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Assignment03 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder.appName("Assignment03").getOrCreate()

    try {
      // Assuming `us_delay_flights_tbl` is a registered table
      // You can register it using: flightsDF.createOrReplaceTempView("us_delay_flights_tbl")

      // Query 1
      val resultQuery1 = spark.sql(
        """SELECT distance, origin, destination 
          |FROM us_delay_flights_tbl 
          |WHERE distance > 1000 
          |ORDER BY distance DESC
          |LIMIT 10""".stripMargin)
      println("Query 1 Result:")
      resultQuery1.show()

      // Query 2
      val resultQuery2 = spark.sql(
        """SELECT date, delay, origin, destination 
          |FROM us_delay_flights_tbl 
          |WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
          |ORDER by delay DESC
          |LIMIT 10""".stripMargin)
      println("Query 2 Result:")
      resultQuery2.show()

    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
