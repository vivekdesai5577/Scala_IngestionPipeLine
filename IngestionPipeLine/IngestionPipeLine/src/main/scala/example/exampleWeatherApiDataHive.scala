package example

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, from_unixtime}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

import java.io.File



object exampleWeatherApiDataHive extends  App {

  val jsonSchema: StructType = new StructType()
    .add("city", StringType)
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

  val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath
  print(warehouseLocation)
  val spark = SparkSession.builder()
    .appName("TestName")
    .master("local[*]")
    .config("hive.metastore.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  var frame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "WeatherData")
    .option("inferSchema","true")
    .load()

  frame.printSchema()
  import spark.implicits._
  frame = frame.selectExpr("CAST(value AS STRING) as JSON_DATA")
    .select(from_json($"JSON_DATA", this.jsonSchema).as("data"))
    .select("data.*")
    .withColumn("dt", from_unixtime(col("dt").as("dt")))
    .withColumn("sunrise", from_unixtime(col("sunrise").as("sunrise")))
    .withColumn("sunset", from_unixtime(col("sunset").as("sunset")))
    .withColumnRenamed("dt","datetime")
    .withColumn("partitiondate", col("datetime").substr(0, 10))

  frame.printSchema()

  frame.createOrReplaceTempView("updates")
  val frame2 = spark.sql("select * from updates")

  val query = frame2.writeStream.foreachBatch { (ds: DataFrame, batchId: Long) =>
     ds.write.insertInto("weather_db.weatherdata")
  }.outputMode("update").start()

  query.awaitTermination()
}
