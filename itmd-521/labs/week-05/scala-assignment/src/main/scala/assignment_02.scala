import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, Dataset, Encoders}
import org.apache.spark.sql.types._
case class DeviceIoTData(
    battery_level: Long,
    c02_level: Long,
    cca2: String,
    cca3: String,
    cn: String,
    device_id: Long,
    device_name: String,
    humidity: Long,
    ip: String,
    latitude: Double,
    lcd: String,
    longitude: Double,
    scale: String,
    temp: Long,
    timestamp: Long
  )
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("assignment_02").config("spark.master", "local").getOrCreate()
    import spark.implicits._

   val schema = StructType(
      Seq(
        StructField("battery_level", LongType, true),
        StructField("c02_level", LongType, true),
        StructField("cca2", StringType, true),
        StructField("cca3", StringType, true),
        StructField("cn", StringType, true),
        StructField("device_id", LongType, true),
        StructField("device_name", StringType, true),
        StructField("humidity", LongType, true),
        StructField("ip", StringType, true),
        StructField("latitude", DoubleType, true),
        StructField("lcd", StringType, true),
        StructField("longitude", DoubleType, true),
        StructField("scale", StringType, true),
        StructField("temp", LongType, true),
        StructField("timestamp", LongType, true)
      )
    )
    val data_set = spark.read.json("/home/vagrant/iot_devices.json")


    // 1. Detect failing devices with battery levels below a threshold.
    val Battery_ThresholdValue = 6
    val failing_Devices = data_set.filter($"battery_level" <  Battery_ThresholdValue )
    println("Failing Devices with battery levels below a threshold:")
    failing_Devices.show()


    // 2. Identify offending countries with high levels of CO2 emissions.
    val Threshold_CO2 = 1000
    val offending_Countries = data_set.filter($"c02_level" > Threshold_CO2 ).select("cn").distinct()
    println("Offending Countries with high levels of CO2 emissions.:")
    offending_Countries.show()



    // 3. Compute the min and max values for temperature, battery level, CO2, and humidity.
    val Maximum_Minimum_Temperature = data_set.agg(min("temp"), max("temp"))
    val Maximum_Minimum_BatteryLevel = data_set.agg(min("battery_level"), max("battery_level"))
    val Maximum_Minimum_CO2_Level = data_set.agg(min("c02_level"), max("c02_level"))
    val Maximum_Minimum_Humidity = data_set.agg(min("humidity"), max("humidity"))
    println("Maximum and Minimum Values  for temperature, battery level, CO2, and humidity.:")
    Maximum_Minimum_Temperature.show()
    Maximum_Minimum_BatteryLevel.show()
    Maximum_Minimum_CO2_Level.show()
    Maximum_Minimum_Humidity.show()


    // 4. Sort and group by average temperature, CO2, humidity, and country
    val Avg_Metrics= data_set.groupBy("cn").agg(avg("temp").alias("Average_temparature"),avg("c02_level").alias("Average_CO2_Level"),avg("humidity").alias("Average_humidity")).sort("Average_temparature", "Average_CO2_Level", "Average_humidity")
    println("Average Metrics by Country:")
    Avg_Metrics.show()



    spark.stop()
  }
}
