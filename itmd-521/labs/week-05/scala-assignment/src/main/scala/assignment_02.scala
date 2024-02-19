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
    val df = spark.read.json("/home/vagrant/iot_devices.json")
    //val ds: Dataset[DeviceIoTData] = df.as[DeviceIoTData]
    //val ds: Dataset[DeviceIoTData] = df.as[DeviceIoTData](Encoders.product[DeviceIoTData])
    //val deviceIoTDataEncoder = Encoders.product[DeviceIoTData]
    // Load the data into a Dataset of DeviceIoTData
    //val df: Dataset[DeviceIoTData] = spark.read.schema(deviceIoTDataEncoder.schema).json("/home/vagrant/iotdevices.json").as(deviceIoTDataEncoder)
    //val df: Dataset[DeviceIoTData] = spark.read.schema(schema).json("/home/vagrant/iotdevices.json").as[DeviceIoTData]
    //val df = spark.read.json("/home/vagrant/iot_devices.json").as[DeviceIoTData]
    //println(df)
    // 1. Detect failing devices with battery levels below a threshold.
    val batteryThreshold = 5
    val failingDevices = df.filter($"battery_level" < batteryThreshold)
    println("Failing Devices:")
    failingDevices.show() 




 

    spark.stop()
  }
}

