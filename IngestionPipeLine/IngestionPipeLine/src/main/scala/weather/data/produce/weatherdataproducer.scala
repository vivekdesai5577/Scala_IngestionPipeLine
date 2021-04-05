package weather.data.produce

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.logging.log4j.core.config.Configurator
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.io.Source.fromURL
import scala.util.{Failure, Success, Try}

//Class for
class weatherdataproducer(dataList: List[String]) {

  //Define logger object for logging messages
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  logger.info(" Initialize weatherdataproducer")
  //Initialize log property file
  //Read log4j.configurationFile property (location) from application.properties
  Configurator.initialize(null, System.getProperty("log4j.configurationFile"))

  //Function to Featch data using URL -
  // Takes no parameters and return JSON data as string format
  def getAPIData(url: String): String = fromURL(url).mkString
  // Filter required data set from the provided data list
  // Takes string and return true if present in dataList otherwise false
  def isin(item: String): Boolean = dataList.foldLeft(false)((r, c) => c.equals(item.split(":")(0).replace("\"","")) || r)
  //Function to format API output JSON data
  // takes JSON as string and return JSON as output
  def formatJSONData(jsonStr: String, city: String): Try[String] = {

    try {
      var json = jsonStr.mkString
        .replace("{", "")
        .replace("}", "")
        .replace("]", "")
        .replace("[", "")
        .replace("\"current\":", "")
        .replace("\"weather\":", "")
        .split(",")
        .toList
      //println(json)
      json = "\"city\":\"%s\"".format(city) :: json.filter(isin)
      //json =  json
      //println(json)
      Success(json.mkString("""{""", ",", """}"""))
    }catch {
      case ex: Throwable => Failure(new Exception(ex.getStackTrace.mkString("\n")))
    }
  }

  //  def isTopicExists(server: String, zookeeperPort: Int, topicName: String): Boolean = {
  //
  //    val sessionTimeOutInMs: Int = 15 * 1000
  //    val connectionTimeOutInMs: Int = 10 * 1000
  //    val zkClient: ZkClient = new ZkClient(server.concat(":").concat(zookeeperPort.toString), sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$)
  //    val zkUtils: ZkUtils = new ZkUtils(zkClient, new ZkConnection(server.concat(":").concat("2181")), false)
  //    AdminUtils.topicExists(zkUtils, topicName)
  //
  //  }
  // Function to write JSON data to Kafka Topic
  def writeToKafka(topic: String, serverName: String, port: Int, city: String, URL: String): Unit = {

    try {
      //Define properties required by kafka
      logger.info(" Pull Api data and add to kafka topic")
      val props = new Properties()
      props.put("bootstrap.servers", serverName.concat(":").concat(port.toString)) // localhost:9092
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      //Create a KafkaProducer which will have (Key, Value) pair

      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

      //Create record of (Key, Value) pair, which placed in configured kafka topics
      //Sample data - {"city":"Mumbai", "lat":18.9667, "lon":72.8333,
      //               "timezone":"Asia/Kolkata", "dt":1617504475, "sunrise":1617498006,
      //               "sunset":1617542573, "temp":300.65, "humidity":74,
      //               "dew_point":295.6, "visibility":2500, "wind_speed":2.06}
      //println(formatJSONData(getAPIData()))
      //println(city)
      formatJSONData(getAPIData(URL), city) match {
        case Success(json: String) =>  {
          val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, "dataJSON", json)
          //Send record
          producer.send(record)
          //close KafkaProducer session
          producer.close()
          logger.debug("Recode added to kafka topic:%s - %s".format(topic, formatJSONData(getAPIData(URL), city)))
          //logger.debug()

        }
        case Failure(ex: Exception) => logger.error(ex.getStackTrace.mkString("\n"))
      }
    }catch {
      case ex: Throwable => logger.error(ex.getStackTrace.mkString("\n"))
    }

  }

}
