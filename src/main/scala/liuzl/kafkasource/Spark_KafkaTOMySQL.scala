package liuzl.kafkasource

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.gson.{JsonObject, JsonParser}
import liuzl.dao.MysqlUtil
import liuzl.pojo.{AgentBean, StrBean}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.format.DateTimeFormat

import java.util.Properties
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter}
import scala.collection.{immutable, mutable}
import scala.util.parsing.json._

object Spark_KafkaTOMySQL {

  def main(args: Array[String]): Unit = {

    //如果从kafka消费数据,启动的线程数至少是2个。
    //其中一个线程负责启动SparkStreaming。另外一个线程负责从kafka消费数据
    //如果只启动一个线程，则无法从kakfa消费数据
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf, Seconds(10))
    conf.registerKryoClasses(Array(classOf[ConsumerRecord[String, String]]))
    ssc.sparkContext.setLogLevel("WARN")

/*    需要采集的topics

      span      	  AIOPS_ETE_SERVFRAMETOPO
      spanChunk	    AIOPS_ETE_SERVCHUNKTOPO
      stat  	      AIOPS_ETE_SERVSTATTOPO
      agent     	  AIOPS_ETE_SERVAGENTTOPO
      api      	    AIOPS_ETE_SERVAPITOPO
      str     	    AIOPS_ETE_SERVSTRTOPO
      sql       	  AIOPS_ETE_SERVSQLTOPO
      unknown  	    AIOPS_ETE_SERVUNKNOWN
      */

    // 定义topic
    val topicsSet = "AIOPS_ETE_SERVFRAMETOPO,AIOPS_ETE_SERVCHUNKTOPO,AIOPS_ETE_SERVSTATTOPO,AIOPS_ETE_SERVAGENTTOPO,AIOPS_ETE_SERVAPITOPO,AIOPS_ETE_SERVSTRTOPO,AIOPS_ETE_SERVSQLTOPO,AIOPS_ETE_SERVUNKNOWN".split(",").toSet

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.165.181:8422,192.168.165.180:8422,192.168.165.179:8422",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "liuzl01",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
//      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    //获取topic中的数据
    val kafkaSource = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    println(kafkaSource)
    println(kafkaSource.toString)

    kafkaSource.foreachRDD{rdd=>
      //lines里存储了当前批次内的所有数据
      val lines = rdd.toLocalIterator
      println(lines)
      while(lines.hasNext) {
        val line = lines.next()
        println(line)
      }
    }

    //处理数据
    kafkaSource.foreachRDD { rdd =>
      //lines里存储了当前批次内的所有数据
      val lines = rdd.toLocalIterator
      println("开始喽！")
      while (lines.hasNext) {
        val line = lines.next() // 获取每一行的数据
        println(line)
        val topics = line.topic()  // 获取对应的topic
        val valJson =  line.value()  // 获取每一行中的value值
        println("value:\t" + valJson)

        if (valJson.startsWith("{") ) {
          // 解析json数据
          val resJson = JSON.parseObject(valJson)

          val 	agentId	= resJson.getString("	agentId	")
          val 	agentStartTime	= resJson.getString("	agentStartTime	")
          val 	stringId	= resJson.getString("	stringId	")
          val 	stringValue	= resJson.getString("	stringValue	")
          val 	setAgentId	= resJson.getString("	setAgentId	")
          val 	setAgentStartTime	= resJson.getString("	setAgentStartTime	")
          val 	setStringId	= resJson.getString("	setStringId	")
          val 	setStringValue	= resJson.getString("	setStringValue	")

          val strBean = StrBean(agentId,agentStartTime,stringId,stringValue,setAgentId,setAgentStartTime,setStringId,setStringValue)

          // 将数据存储到MySQL
          MysqlUtil.saveTo_strBean(strBean)

/*          // 获取Json数据中的对应值
          val		hostname		= resJson.getString("hostname")
          val		ip		= resJson.getString("ip")
          val		ports		= resJson.getString("ports")
          val		agentId		= resJson.getString("agentId")
          val		applicationName		= resJson.getString("applicationName")
          val		serviceType		= resJson.getString("serviceType")
          val		pid		= resJson.getString("pid")
          val		agentVersion		= resJson.getString("agentVersion")
          val		vmVersion		= resJson.getString("vmVersion")
          val		startTimestamp		= resJson.getString("startTimestamp")
          val		endTimestamp		= resJson.getString("endTimestamp")
          val		endStatus		= resJson.getString("endStatus")
          val		serverMetaData		= resJson.getString("serverMetaData")
          val		jvmInfo		= resJson.getString("jvmInfo")
          val		setServerMetaData		= resJson.getString("setServerMetaData")
          val		setJvmInfo		= resJson.getString("setJvmInfo")
          val		setHostname		= resJson.getString("setHostname")
          val		setIp		= resJson.getString("setIp")
          val		setPorts		= resJson.getString("setPorts")
          val		setAgentId		= resJson.getString("setAgentId")
          val		setApplicationName		= resJson.getString("setApplicationName")
          val		setServiceType		= resJson.getString("setServiceType")
          val		setPid		= resJson.getString("setPid")
          val		setAgentVersion		= resJson.getString("setAgentVersion")
          val		setVmVersion		= resJson.getString("setVmVersion")
          val		setStartTimestamp		= resJson.getString("setStartTimestamp")
          val		setEndTimestamp		= resJson.getString("setEndTimestamp")
          val		setEndStatus		= resJson.getString("setEndStatus")

          // 写入Bean中
          val   agentBean = AgentBean(hostname,ip,ports,agentId,applicationName,serviceType,pid,agentVersion,
            vmVersion,startTimestamp,endTimestamp,endStatus,serverMetaData,jvmInfo,setServerMetaData,
            setJvmInfo,setHostname,setIp,setPorts,setAgentId,setApplicationName,setServiceType,setPid,
            setAgentVersion,setVmVersion,setStartTimestamp,setEndTimestamp,setEndStatus)

          // 将数据存储到MySQL
          MysqlUtil.saveToMysql(agentBean, topics)*/

          println()
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
