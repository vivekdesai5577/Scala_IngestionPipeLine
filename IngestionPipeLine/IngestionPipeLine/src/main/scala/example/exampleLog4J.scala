package example

import org.slf4j.LoggerFactory
import org.apache.logging.log4j.core.config.Configurator
import weather.data.produce.weatherapidata.{getClass, prop}

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties

object exampleLog4J extends App {

  val logger = LoggerFactory.getLogger(getClass().getName())

  //Configurator.initialize(null, "src/main/resources/log4j2.properties")
  Configurator.initialize(null, System.getProperty("log4j.configurationFile"))

  val prop = new Properties()
  //prop.load(new FileInputStream("src/main/resources/application.properties"))
  prop.load(new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/application.properties"))))

  val input = prop.getProperty("path.input")
  val output = prop.getProperty("path.output")

  logger.info("Test Message")
  println(output)
  println(input)
  logger.info(input)
  logger.info(output)

}
