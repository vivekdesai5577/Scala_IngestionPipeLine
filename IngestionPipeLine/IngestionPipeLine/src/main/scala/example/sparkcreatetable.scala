package example

import example.exampleWeatherApiDataHive.warehouseLocation
import org.apache.spark.sql.SparkSession

import java.io.File

object sparkcreatetable extends App {
  val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath
print(warehouseLocation)
  val spark = SparkSession.builder()
    .appName("TestName")
    .master("local[*]")
    .config("hive.metastore.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

//  spark.sql("create database weather_db")
//  spark.sql("create table weather_db.weatherdata(city string, lat double, lon double, timezone string, eventtime timestamp, sunrise timestamp, sunset timestamp, temp timestamp, humidity int, dew_point double, visibility int, wind_speed double) PARTITIONED by (partitiondate string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED as parquet")
//  spark.sql("insert into weather_db.weatherdata values('Delhi',28.66,77.23,'Asia/Kolkata','2021-04-05 15:29:56','2021-04-05 06:06:28','2021-04-05 18:40:45',310.41,19,282.93,4000,3.09,'2021-04-05')")
//  spark.sql("select * from weather_db.weatherdata").show()
  //spark.sql("use weather_db")
//
//  spark.sql("create table weather_db.weatherdata(city string, lat double, lon double, timezone string, eventtime timestamp, sunrise timestamp, sunset timestamp, temp timestamp, humidity int, dew_point double, visibility int, wind_speed double) PARTITIONED by (partitiondate string) STORED as parquet")
  spark.sql("Desc weather_db.weatherdata").show(truncate=false)
    spark.sql("insert into weather_db.weatherdata values('Delhi',28.66,77.23,'Asia/Kolkata','2021-04-05 15:29:56','2021-04-05 06:06:28','2021-04-05 18:40:45',310.41,19,282.93,4000,3.09,'2021-04-05')")
    spark.sql("select * from weather_db.weatherdata").show(truncate=false)
}
