package weather.data.consume

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.functions.{col, from_json, from_unixtime}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source.fromURL

class weatherdataprocessing {

  //Define schema for streaming kafka json data using spark
  val jsonSchema: StructType = new StructType()
    .add("lat", DoubleType)
    .add("lon",DoubleType)
    .add("timezone", StringType)
    .add("dt", LongType)
    .add("sunrise", LongType)
    .add("sunset", LongType)
    .add("temp", DoubleType)
    .add("humidity", IntegerType)
    .add("dew_point", DoubleType)
    .add("visibility", IntegerType)
    .add("wind_speed", DoubleType)


  //Function to create spark session,
  //Takes 2 parameters application name and master compute mode
  //Returns  SparkSession object
  def createSparkSession(appName: String, master: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

  def readStramDataFromkafka(spark: SparkSession, source: String, serverName: String, port: Int, topic: String): DataFrame =  {
    spark
      .readStream
      .format(source)
      .option("kafka.bootstrap.servers", serverName.concat(":").concat(port.toString))
      .option("subscribe", topic)
      .option("inferSchema","true")
      .load()
  }
  def convertJsonDataToTabular(frame: DataFrame, spark: SparkSession): DataFrame = {

    frame.printSchema()
    import spark.implicits._
    frame.selectExpr("CAST(value AS STRING) as JSON_DATA")
      .select(from_json($"JSON_DATA", this.jsonSchema).as("data"))
      .select("data.*")
      .withColumn("dt", from_unixtime(col("dt").as("dt")))
      .withColumn("sunrise", from_unixtime(col("sunrise").as("sunrise")))
      .withColumn("sunset", from_unixtime(col("sunset").as("sunset")))
      .withColumnRenamed("dt","datetime")
      .withColumn("partationdate", col("datetime").substr(0, 10))
  }
  def writeStreamDataToConsole(frame: DataFrame): StreamingQuery = {

    frame.writeStream
      .outputMode("update")
      .format("console")
      .start()
  }
  def writeStreamDataToParquet(frame: DataFrame): StreamingQuery = {

    frame.writeStream
      .format("parquet")
      .option("checkpointLocation", "D:\\CheckPointWeatherData")
      .option("path", "D:\\OutputWeatherData")
      .partitionBy("partationdate")
      .outputMode("append")
      .start()
  }
}