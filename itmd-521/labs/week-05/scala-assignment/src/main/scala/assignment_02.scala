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




    spark.stop()
  }
}
