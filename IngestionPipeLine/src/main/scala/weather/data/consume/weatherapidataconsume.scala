package weather.data.consume

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object weatherapidataconsume extends App {

  val consumeData: weatherdataprocessing = new weatherdataprocessing()

  val spark = consumeData.createSparkSession("WeatherData", "spark://vivekdesai:7077")
  spark.sparkContext.setLogLevel("ERROR")
  var frame: DataFrame = consumeData.readStramDataFromkafka(spark, "kafka",
    "localhost", 9092, "WeatherData")

  frame = consumeData.convertJsonDataToTabular(frame, spark)
  frame.printSchema()

  val query: StreamingQuery = consumeData.writeStreamDataToConsole(frame)
  //val query1: StreamingQuery = consumeData.writeStreamDataToParquet(frame)

  query.awaitTermination()
  //query1.awaitTermination()


}
