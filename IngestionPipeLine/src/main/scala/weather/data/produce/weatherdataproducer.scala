package weather.data.produce

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source.fromURL
//import org.I0Itec.zkclient.ZkClient
//import kafka.utils.ZKStringSerializer$
//import kafka.utils.ZkUtils
//import org.I0Itec.zkclient.ZkConnection
import java.util.Properties

class weatherdataproducer(url: String, dataList: List[String]) {

  //Function to Featch data using URL -
  // Takes no parameters and return JSON data as string format
  def getAPIData(): String = fromURL(url).mkString
  // Filter required data set from the provided data list
  // Takes string and return true if present in dataList otherwise false
  def isin(item: String): Boolean = dataList.foldLeft(false)((r, c) => c.equals(item.split(":")(0).replace("\"","")) || r)
  //Function to format API output JSON data
  // takes JSON as string and return JSON as output
  def formatJSONData(jsonStr: String): String = {
    println(jsonStr)
    var json = jsonStr.mkString
      .replace("{", "")
      .replace("}", "")
      .replace("]", "")
      .replace("[", "")
      .replace("\"current\":", "")
      .replace("\"weather\":", "")
      .split(",")
      .toList
    println(json)
    json.filter(isin).mkString("""{""", ",", """}""")
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
  def writeToKafka(topic: String, serverName: String, port: Int): Unit = {
    //Define properties required by kafka
    val props = new Properties()
    props.put("bootstrap.servers", serverName.concat(":").concat(port.toString)) // localhost:9092
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    //Create a KafkaProducer which will have (Key, Value) pair

    val producer:  KafkaProducer[String, String] = new KafkaProducer[String, String](props)

    //Create record of (Key, Value) pair, which placed in configured kafka topics
    println(formatJSONData(getAPIData()))
    val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic,"dataJSON", formatJSONData(getAPIData()))
    //Send record
    producer.send(record)
    //close KafkaProducer session
    producer.close()
  }
}
