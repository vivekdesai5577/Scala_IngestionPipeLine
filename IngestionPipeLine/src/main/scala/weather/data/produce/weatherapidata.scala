package weather.data.produce

import org.apache.logging.log4j.core.config.Configurator
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties

object weatherapidata extends App {

  //Define logger object for logging messages
  val logger: Logger = LoggerFactory.getLogger(getClass().getName())
  //Initialize log property file
  //Configurator.initialize(null, "src/main/resources/log4j2.properties")
  //val logConfig: PropertiesConfiguration = new PropertiesConfiguration(System.getProperty("log4j.configurationFile"))
  Configurator.initialize(null, System.getProperty("log4j.configurationFile"))

  //Define property config file for reading properties
  val prop: Properties = new Properties()
  //prop.load(new FileInputStream("src/main/resources/application.properties"))

  prop.load(new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/application.properties"))))

  try {
    //Read all properties related to API URL
    val URL: String = prop.getProperty("API.URL") // URL, hostname
    val exclude: String = prop.getProperty("API.exclude") // Exclude elements
    val appid: String = prop.getProperty("API.appid") // API token id
    val latitude: String = prop.getProperty("API.lat") // City latitude
    val longitude: String = prop.getProperty("API.lon") // City longitude
    val unit: String = prop.getProperty("API.units") // Measurement Unit
    // Create URL
    val ApiURL: String = "%s?lat=%s&lon=%s&exclude=%s&appid=%s&units=metrics".format(URL, latitude, longitude, exclude, appid, unit)
    logger.info("APIurl:" + ApiURL)

    //Data which needs to extracted from the json output (API output)
    //val dataList: List[String] = List(prop.getProperty("API.dataList"))
    val dataList: List[String] = List("lat", "lon", "timezone", "dt", "sunrise", "sunset", "temp", "humidity", "dew_point", "visibility", "wind_speed")
    print(dataList)
    logger.info("APIRequiredData:" + dataList.mkString(","))

    //Pull data for every 10 seconds
    val timeMS: Int = (prop.getProperty("API.polltime")).trim.toInt
    logger.info("APIDataPollTime:" + timeMS + "ms")

    //Kafka confiuration details
    val server: String = prop.getProperty("kafka.server")
    val port: Int = (prop.getProperty("kafka.port")).trim.toInt
    val zookeeperPort: Int = (prop.getProperty("kafka.zookeeperPort")).trim.toInt
    val topicName: String = prop.getProperty("kafka.topicName")
    logger.info("KafkaServer:" + server)
    logger.info("KafkaPort:" + port)

    val produceData: weatherdataproducer = new weatherdataproducer(ApiURL, dataList)

    val Flag: Boolean = true
    while (Flag) {
      produceData.writeToKafka(topicName, server, port)
      logger.info("API Poll Completed and JsonData is put on KafkaTopic")
      logger.info("SleepFor:" + timeMS)
      Thread.sleep(timeMS)
    }

  } catch {
    case ex: Throwable => {
      logger.error(ex.getStackTrace.mkString("\n"))
    }
  }

}
