package example

import weather.data.produce.weatherapidata.{getClass, logger, prop}
import weather.data.produce.weatherdataproducer

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties
import scala.io.BufferedSource

object exampleLog4J extends App {

  //Define property config file for reading properties
  val prop: Properties = new Properties()
  //prop.load(new FileInputStream("src/main/resources/application.properties"))
  prop.load(new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/application.properties"))))

  val URL: String = prop.getProperty("API.URL") // URL, hostname
  val exclude: String = prop.getProperty("API.exclude") // Exclude elements
  val appid: String = prop.getProperty("API.appid") // API token id
  val unit: String = prop.getProperty("API.units") // Measurement Unit

  //Read City Data - Name, latitude, longitude
  val CityListSource = scala.io.Source.fromFile("src/main/resources/CityList")
  var ApiURL: String = ""
  val dataList: List[String] = List("lat", "lon", "timezone", "dt", "sunrise", "sunset", "temp", "humidity", "dew_point", "visibility", "wind_speed")
  println(dataList)
  val timeMS: Int = (prop.getProperty("API.polltime")).trim.toInt
  println(timeMS)

  //Kafka confiuration details
  val server: String = prop.getProperty("kafka.server")
  val port: Int = (prop.getProperty("kafka.port")).trim.toInt
  val zookeeperPort: Int = (prop.getProperty("kafka.zookeeperPort")).trim.toInt
  val topicName: String = prop.getProperty("kafka.topicName")

  val produceData: weatherdataproducer = new weatherdataproducer(dataList)

  val Flag: Boolean = true
  println(Flag)
  val cityLines: Seq[String] = CityListSource.getLines.toSeq

  while(true) {
    cityLines.foreach({
      line => println("city - %s".format(line.split(",")(0)))
        ApiURL = "%s?lat=%s&lon=%s&exclude=%s&appid=%s&units=metrics".format(URL, line.split(",")(1), line.split(",")(2), exclude, appid, unit)
        println(ApiURL)
        produceData.writeToKafka(topicName, server, port, line.split(",")(0), ApiURL)
    })
    println(" Sleep for 10 secs ----")
    Thread.sleep(timeMS)
  }


}