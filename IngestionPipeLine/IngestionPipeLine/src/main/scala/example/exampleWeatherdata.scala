package example

import org.apache.logging.log4j.core.config.Configurator
import org.slf4j.{Logger, LoggerFactory}
import weather.data.produce.weatherapidata.{getClass, logger, prop}
import weather.data.produce.weatherdataproducer
import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties

//weatherapidata is a class for polling data for each of yje cities
//Takes two constructures-
//                       -Logger : log object for writing messgaes to log files
//                       -prop : Property object to read all properties from application.properties
//Class function -
//               -logallclassvariables : function logs all class variables logs files
//               -pollapidata : Polls data for each of the cities
class weatherapidata(logger: Logger, prop: Properties){

  logger.info("Read and initialize all properties")
  //Read all properties related to API URL
  //API Related properties
  //Example of URL - https://api.openweathermap.org/data/2.5/onecall?lat=28.66&lon=77.23&exclude=minutely,hourly,daily,alerts&appid=7f9e75dbfba0b07fe2e4e79fc4457342&units=metrics
  val URL: String = prop.getProperty("API.URL") // URL, hostname
  val exclude: String = prop.getProperty("API.exclude") // Exclude elements
  val appid: String = prop.getProperty("API.appid") // API token id
  val unit: String = prop.getProperty("API.units") // Measurement Unit
  //Define list of columns for which data has to be extracted.
  val dataList: List[String] = List("lat", "lon", "timezone", "dt", "sunrise", "sunset", "temp", "humidity", "dew_point", "visibility", "wind_speed")
  //Pull data for every mil seconds
  val timeMS: Int = (prop.getProperty("API.polltime")).trim.toInt
  //Kafka configuration details
  val server: String = prop.getProperty("kafka.server") // Server ip or hostname
  val port: Int = (prop.getProperty("kafka.port")).trim.toInt // kafka Port number
  val zookeeperPort: Int = (prop.getProperty("kafka.zookeeperPort")).trim.toInt //zookeeper port
  val topicName: String = prop.getProperty("kafka.topicName") // Kafka topic name
  //Boolen for while loop
  val Flag: Boolean = true
  //Read City Data - Name, latitude, longitude
  val CityListSource = scala.io.Source.fromFile("src/main/resources/CityList")

  //Function to log all properties to log file
  def logallclassvariables(): Unit = this.getClass.getDeclaredFields.foreach(field => logger.info("Variable Name:%s - value:%s".format(field.getName, field.get(this))))

  //Function to to poll API data
  //      Its polls data for each of the cities/
  //      Polled data is written to kafka topic from where spark application reads the data.
  def pollapidata(): Unit ={

    try{

      //Define produceData class object which function writeToKafka, which will poll data and then same written to kafka topic.
      val produceData: weatherdataproducer = new weatherdataproducer(dataList)
      //Read CityList csv file and convert it to Sequence collection having Strings which can be repeatedly traversed.
      val cityLines: Seq[String] = CityListSource.getLines.toSeq
      //Variables which hols URL to poll data
      var ApiURL = ""
      while(true) { //Unconditional loop - loops every defines mil-seconds.
        cityLines.foreach({ // Loop through each of the cities
          line => logger.info("City:%s - API Polling started".format((line.split(",")(0))))
            //Example - https://api.openweathermap.org/data/2.5/onecall?lat=28.66&lon=77.23&exclude=minutely,hourly,daily,alerts&appid=7f9e75dbfba0b07fe2e4e79fc4457342&units=metrics
            ApiURL = "%s?lat=%s&lon=%s&exclude=%s&appid=%s&units=metrics".format(URL, line.split(",")(1), line.split(",")(2), exclude, appid, unit)
            println(ApiURL)
            logger.info("APIurl - " + ApiURL)
            produceData.writeToKafka(topicName, server, port, line.split(",")(0), ApiURL)
            logger.info("City:%s - API Polling completed".format((line.split(",")(0))))
        })
        //println(" Sleep for 10 secs ----")
        logger.info("Sleep for %d ms".format(timeMS))
        Thread.sleep(timeMS)
      }
    }catch {
      case ex: Throwable => {
        logger.error(ex.getStackTrace.mkString("\n"))
      }
    }
  }
}

//Object call with main metod, process starts from this line.
object exampleWeatherdata extends App{

  //Define logger object for logging messages
  val logger: Logger = LoggerFactory.getLogger(getClass().getName())
  //Initialize log property file
  //Read log4j.configurationFile property (location) from application.properties
  Configurator.initialize(null, System.getProperty("log4j.configurationFile"))

  //Define property config file for reading properties
  val prop: Properties = new Properties()
  //Load application.property file
  prop.load(new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/application.properties"))))

  try {
    val weatherapi = new weatherapidata(logger, prop)
    weatherapi.logallclassvariables
    weatherapi.pollapidata
  }catch {
    case ex: Throwable => {
      logger.error(ex.getStackTrace.mkString("\n"))
    }
  }
}
