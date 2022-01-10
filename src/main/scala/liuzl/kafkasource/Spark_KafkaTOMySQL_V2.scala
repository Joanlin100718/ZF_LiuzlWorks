package liuzl.kafkasource

import com.alibaba.fastjson.JSON
import liuzl.dao.MysqlUtil
import liuzl.pojo.{AgentBean, AgentTailBean, StrBean}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark_KafkaTOMySQL_V2 {

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
//    val topicsSet = "AIOPS_ETE_SERVFRAMETOPO,AIOPS_ETE_SERVCHUNKTOPO,AIOPS_ETE_SERVSTATTOPO,AIOPS_ETE_SERVAGENTTOPO,AIOPS_ETE_SERVAPITOPO,AIOPS_ETE_SERVSTRTOPO,AIOPS_ETE_SERVSQLTOPO,AIOPS_ETE_SERVUNKNOWN".split(",").toSet
    val topicsSet = "AIOPS_ETE_SERVSTRTOPO,AIOPS_ETE_SERVAGENTTOPO".split(",").toSet

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

    //处理数据
    kafkaSource.foreachRDD { rdd =>
      //lines里存储了当前批次内的所有数据
      val lines = rdd.toLocalIterator

      while (lines.hasNext) {
        val line = lines.next() // 获取每一行的数据
//        println(line + "\n")
        val topics = line.topic()  // 获取对应的topic
        val valJson =  line.value()  // 获取每一行中的value值
//        println("topic:\t" + topics)
//        println("value:\t" + valJson)
        val topic = "topic_" + topics
        println(topic)
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

        if (topics.contains("AIOPS_ETE_SERVAGENTTOPO")){
          // 解析JSON
          val resJson = JSON.parseObject(valJson)
          // 获取数据
          val		hostName		= resJson.getString("hostName")

          if ( hostName != null) {
            val		hostName		= resJson.getString("hostName")
            val		ip		= resJson.getString("ip")
            val		ports		= resJson.getString("ports")
            val		agentId		= resJson.getString("agentId")
            val		applicationName		= resJson.getString("applicationName")
            val		serviceTypeCode		= resJson.getString("serviceTypeCode")
            val		pid		= resJson.getString("pid")
            val		agentVersion		= resJson.getString("agentVersion")
            val		vmVersion		= resJson.getString("vmVersion")
            val		startTime		= resJson.getString("startTime")
            val		endTimestamp		= resJson.getString("endTimeStamp")
            val		endStatus		= resJson.getString("endStatus")
            val		container		= resJson.getString("container")
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
            val agentBean = AgentBean(hostName, ip, ports, agentId, applicationName, serviceTypeCode, pid, agentVersion,
              vmVersion, startTime, endTimestamp, endStatus, container, serverMetaData, jvmInfo, setServerMetaData,
              setJvmInfo, setHostname, setIp, setPorts, setAgentId, setApplicationName, setServiceType, setPid,
              setAgentVersion, setVmVersion, setStartTimestamp, setEndTimestamp, setEndStatus)
            // 将数据存储到MySQL
            println(topics)
            MysqlUtil.saveTo_agentBean(agentBean )
          } else {

            val		version		= resJson.getString("version")
            val		agentId		= resJson.getString("agentId")
            val		startTimestamp		= resJson.getString("startTimestamp")
            val		eventTimestamp		= resJson.getString("eventTimestamp")
            val		eventIdentifier		= resJson.getString("eventIdentifier")
            val		agentLifeCycleState		= resJson.getString("agentLifeCycleState")

            val agentTailBean = AgentTailBean(version,agentId,startTimestamp,eventTimestamp,eventIdentifier,agentLifeCycleState)
            // 将数据存储到MySQL

            println(topics)
            MysqlUtil.saveTo_agentTailBean(agentTailBean )

          }
        } else if (topics.contains("AIOPS_ETE_SERVSTRTOPO")){
          // 解析JSON
          val resJson = JSON.parseObject(valJson)
          // 获取数据
          val agentId = resJson.getString("agentId")
          val startTime = resJson.getString("startTime")
          val stringId = resJson.getString("stringId")
          val stringValue = resJson.getString("stringValue")
          val setAgentId = resJson.getString("setAgentId")
          val setAgentStartTime = resJson.getString("setAgentStartTime")
          val setStringId = resJson.getString("setStringId")
          val setStringValue = resJson.getString("setStringValue")
          val strBean = StrBean(agentId, startTime, stringId, stringValue, setAgentId, setAgentStartTime, setStringId, setStringValue)

          println(topic)
          // 将数据存储到MySQL
          MysqlUtil.saveTo_strBean(strBean)
        }



//        topic match{
//          case topic_AIOPS_ETE_SERVAGENTTOPO => {
//            // 解析JSON
//            val resJson = JSON.parseObject(valJson)
//            // 获取数据
//            val		hostName		= resJson.getString("hostName")
//
//            if ( hostName != null) {
//              val		hostName		= resJson.getString("hostName")
//              val		ip		= resJson.getString("ip")
//              val		ports		= resJson.getString("ports")
//              val		agentId		= resJson.getString("agentId")
//              val		applicationName		= resJson.getString("applicationName")
//              val		serviceTypeCode		= resJson.getString("serviceTypeCode")
//              val		pid		= resJson.getString("pid")
//              val		agentVersion		= resJson.getString("agentVersion")
//              val		vmVersion		= resJson.getString("vmVersion")
//              val		startTime		= resJson.getString("startTime")
//              val		endTimestamp		= resJson.getString("endTimeStamp")
//              val		endStatus		= resJson.getString("endStatus")
//              val		container		= resJson.getString("container")
//              val		serverMetaData		= resJson.getString("serverMetaData")
//              val		jvmInfo		= resJson.getString("jvmInfo")
//              val		setServerMetaData		= resJson.getString("setServerMetaData")
//              val		setJvmInfo		= resJson.getString("setJvmInfo")
//              val		setHostname		= resJson.getString("setHostname")
//              val		setIp		= resJson.getString("setIp")
//              val		setPorts		= resJson.getString("setPorts")
//              val		setAgentId		= resJson.getString("setAgentId")
//              val		setApplicationName		= resJson.getString("setApplicationName")
//              val		setServiceType		= resJson.getString("setServiceType")
//              val		setPid		= resJson.getString("setPid")
//              val		setAgentVersion		= resJson.getString("setAgentVersion")
//              val		setVmVersion		= resJson.getString("setVmVersion")
//              val		setStartTimestamp		= resJson.getString("setStartTimestamp")
//              val		setEndTimestamp		= resJson.getString("setEndTimestamp")
//              val		setEndStatus		= resJson.getString("setEndStatus")
//
//              // 写入Bean中
//              val agentBean = AgentBean(hostName, ip, ports, agentId, applicationName, serviceTypeCode, pid, agentVersion,
//                vmVersion, startTime, endTimestamp, endStatus, container, serverMetaData, jvmInfo, setServerMetaData,
//                setJvmInfo, setHostname, setIp, setPorts, setAgentId, setApplicationName, setServiceType, setPid,
//                setAgentVersion, setVmVersion, setStartTimestamp, setEndTimestamp, setEndStatus)
//              // 将数据存储到MySQL
//              println(topics)
//              MysqlUtil.saveTo_agentBean(agentBean )
//            } else {
//
//              val		version		= resJson.getString("version")
//              val		agentId		= resJson.getString("agentId")
//              val		startTimestamp		= resJson.getString("startTimestamp")
//              val		eventTimestamp		= resJson.getString("eventTimestamp")
//              val		eventIdentifier		= resJson.getString("eventIdentifier")
//              val		agentLifeCycleState		= resJson.getString("agentLifeCycleState")
//
//              val agentTailBean = AgentTailBean(version,agentId,startTimestamp,eventTimestamp,eventIdentifier,agentLifeCycleState)
//              // 将数据存储到MySQL
//
//              println(topics)
//              MysqlUtil.saveTo_agentTailBean(agentTailBean )
//
//            }
//          }
//          case topic_AIOPS_ETE_SERVSTRTOPO => {
//            // 解析JSON
//            val resJson = JSON.parseObject(valJson)
//             // 获取数据
//            val agentId = resJson.getString("agentId")
//            val startTime = resJson.getString("startTime")
//            val stringId = resJson.getString("stringId")
//            val stringValue = resJson.getString("stringValue")
//            val setAgentId = resJson.getString("setAgentId")
//            val setAgentStartTime = resJson.getString("setAgentStartTime")
//            val setStringId = resJson.getString("setStringId")
//            val setStringValue = resJson.getString("setStringValue")
//            val strBean = StrBean(agentId, startTime, stringId, stringValue, setAgentId, setAgentStartTime, setStringId, setStringValue)
//
//            println(topic)
//            // 将数据存储到MySQL
//            MysqlUtil.saveTo_strBean(strBean)
//          }
//        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
