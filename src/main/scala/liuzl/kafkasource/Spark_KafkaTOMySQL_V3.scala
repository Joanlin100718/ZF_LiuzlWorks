package liuzl.kafkasource

import com.alibaba.fastjson.JSON
import liuzl.dao.MysqlUtil
import liuzl.pojo.{UnknownBean, _}
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date


/*
*
*   这个版本的读取数据，需要从头开始遍历，所以耗时较长
*
*   这个版本，每次重启会消耗大量的时间
*
* */

object Spark_KafkaTOMySQL_V3 {

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

    // 定义topic列表
    val topicsSet = (
                      "AIOPS_ETE_SERVFRAMETOPO,"  +  //span
                      "AIOPS_ETE_SERVCHUNKTOPO,"  +   //spanChunk
//                      "AIOPS_ETE_SERVSTATTOPO,"   +   //stat
                      "AIOPS_ETE_SERVAGENTTOPO," +    //agent
                      "AIOPS_ETE_SERVAPITOPO," +     //api
                      "AIOPS_ETE_SERVSTRTOPO,"  +     //str
                      "AIOPS_ETE_SERVUNKNOWN,"   +     // unknown
                      "AIOPS_ETE_SERVSQLTOPO,"       //sql
      ).split(",").toSet
//    val topicsSet = "AIOPS_ETE_SERVSQLTOPO".split(",").toSet

    // 添加topic配置
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.165.181:8422,192.168.165.180:8422,192.168.165.179:8422",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "Liuzl",
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

        val offset = line.offset()        // 获取对应的offset
        val partition = line.partition()  // 获取对应的partition编号
        val topics = line.topic()         // 获取对应的topic
        val valJson =  line.value() // 获取每一行中的value值

        if (MysqlUtil.selectAndUpdateOffset(topics , offset , partition)) {

          // 打印数据列表
          print("数据分区：" + partition)
          printTopic(topics)
          println("数据列表：" + line)
          //        println("topic:\t" + topics)
          //        println("value:\t" + valJson)

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

          // 根据topic判断，处理topic列表中的每一个topic
          if (topics.equals("AIOPS_ETE_SERVAGENTTOPO")){   // agent     	  AIOPS_ETE_SERVAGENTTOPO
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

            // 获取当前时间
            getTime()

            // 将数据存储到MySQL
            MysqlUtil.saveTo_agent(agentBean )
          } else {

            val		version		= resJson.getString("version")
            val		agentId		= resJson.getString("agentId")
            val		startTimestamp		= resJson.getString("startTimestamp")
            val		eventTimestamp		= resJson.getString("eventTimestamp")
            val		eventIdentifier		= resJson.getString("eventIdentifier")
            val		agentLifeCycleState		= resJson.getString("agentLifeCycleState")

            val agentTailBean = AgentTailBean(version,agentId,startTimestamp,eventTimestamp,eventIdentifier,agentLifeCycleState)

            // 获取当前时间
            getTime()

            // 将数据存储到MySQL
            MysqlUtil.saveTo_agentTail(agentTailBean )
          }
        } else if (topics.equals("AIOPS_ETE_SERVSTRTOPO")){ // str    AIOPS_ETE_SERVSTRTOPO
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

          // 获取当前时间
          getTime()

          // 将数据存储到MySQL
          MysqlUtil.saveTo_str(strBean)
        } else if (topics.equals("AIOPS_ETE_SERVFRAMETOPO")){  //span      	  AIOPS_ETE_SERVFRAMETOPO
          // 解析JSON
          val resJson = JSON.parseObject(valJson)
          // 获取数据
          val	version	= resJson.getString("version")
          val	agentId	= resJson.getString("agentId")
          val	applicationId	= resJson.getString("applicationId")
          val	agentStartTime	= resJson.getString("agentStartTime")
          val	transactionId	= resJson.getString("transactionId")
          val	spanId	= resJson.getString("spanId")
          val	parentSpanId	= resJson.getString("parentSpanId")
          val	parentApplicationId	= resJson.getString("parentApplicationId")
          val	parentApplicationServiceType	= resJson.getString("parentApplicationServiceType")
          val	startTime	= resJson.getString("startTime")
          val	elapsed	= resJson.getString("elapsed")
          val	rpc	= resJson.getString("rpc")
          val	serviceType	= resJson.getString("serviceType")
          val	endPoint	= resJson.getString("endPoint")
          val	apiId	= resJson.getString("apiId")
          val	annotationBoList	= resJson.getString("annotationBoList")
          val	flag	= resJson.getString("flag")
          val	errCode	= resJson.getString("errCode")
          val	spanEventBoList	= resJson.getString("spanEventBoList")
          val	collectorAcceptTime	= resJson.getString("collectorAcceptTime")
          val	exceptionId	= resJson.getString("exceptionId")
          val	exceptionMessage	= resJson.getString("exceptionMessage")
          val	exceptionClass	= resJson.getString("exceptionClass")
          val	applicationServiceType	= resJson.getString("applicationServiceType")
          val	acceptorHost	= resJson.getString("acceptorHost")
          val	remoteAddr	= resJson.getString("remoteAddr")
          val	loggingTransactionInfo	= resJson.getString("loggingTransactionInfo")
          val	root	= resJson.getString("root")
          val	rawVersion	= resJson.getString("rawVersion")
          val spanBean = SpanBean(version,agentId,applicationId,agentStartTime,transactionId,spanId,parentSpanId,parentApplicationId,parentApplicationServiceType,startTime,elapsed,rpc,serviceType,endPoint,apiId,annotationBoList,flag,errCode,spanEventBoList,collectorAcceptTime,exceptionId,exceptionMessage,	exceptionClass,applicationServiceType,acceptorHost,remoteAddr,loggingTransactionInfo,root,rawVersion)

          // 获取当前时间
          getTime()

          // 将数据存储到MySQL
          MysqlUtil.saveTo_span(spanBean)
        } else if (topics.equals("AIOPS_ETE_SERVCHUNKTOPO")){  //spanChunk	    AIOPS_ETE_SERVCHUNKTOPO
          // 解析JSON
          val resJson = JSON.parseObject(valJson)
          // 获取数据
          val	version	= resJson.getString("version")
          val	agentId	= resJson.getString("agentId")
          val	applicationId	= resJson.getString("applicationId")
          val	agentStartTime	= resJson.getString("agentStartTime")
          val	transactionId	= resJson.getString("transactionId")
          val	spanId	= resJson.getString("spanId")
          val	endPoint	= resJson.getString("endPoint")
          val	serviceType	= resJson.getString("serviceType")
          val	applicationServiceType	= resJson.getString("applicationServiceType")
          val	spanEventBoList	= resJson.getString("spanEventBoList")

          val spanChuckBean = SpanChunkBean(version,agentId,applicationId,agentStartTime,transactionId,spanId,endPoint,serviceType,applicationServiceType,spanEventBoList)

          // 获取当前时间
          getTime()
          // 将数据存储到MySQL
          MysqlUtil.saveTo_spanChunk(spanChuckBean)

        } else if (topics.equals("AIOPS_ETE_SERVAPITOPO")){  //api      	    AIOPS_ETE_SERVAPITOPO
          // 解析JSON
          val resJson = JSON.parseObject(valJson)
          // 获取数据
          val	agentId	= resJson.getString("agentId")
          val	startTime	= resJson.getString("startTime")
          val	apiId	= resJson.getString("apiId")
          val	apiInfo	= resJson.getString("apiInfo")
          val	lineNumber	= resJson.getString("lineNumber")
          val	methodTypeEnum = resJson.getString("methodTypeEnum")
          val	description	= resJson.getString("description")
          val	setLine	= resJson.getString("setLine")
          val	setType	= resJson.getString("setType")
          val	setAgentId	= resJson.getString("setAgentId")
          val	setAgentStartTime	= resJson.getString("setAgentStartTime")
          val	setApiId	= resJson.getString("setApiId")
          val	setApiInfo	= resJson.getString("setApiInfo")


          val apikBean = ApiBean(agentId,startTime,apiId,apiInfo,lineNumber,methodTypeEnum,description,setLine,setType,setAgentId,setAgentStartTime,setApiId,setApiInfo)

          // 获取当前时间
          getTime()
          // 将数据存储到MySQL
          MysqlUtil.saveTo_api(apikBean)
        } else if (topics.equals("AIOPS_ETE_SERVSQLTOPO")){  //sql       	  AIOPS_ETE_SERVSQLTOPO
          // 解析JSON
          val resJson = JSON.parseObject(valJson)
          // 获取数据
          val	agentId	= resJson.getString("agentId")
          val	startTime	= resJson.getString("startTime")
          val	sqlId	= resJson.getString("sqlId")
          val	sql	= resJson.getString("sql")
          val	hashCode	= resJson.getString("hashCode")
          val	setAgentId	= resJson.getString("setAgentId")
          val	setAgentStartTime	= resJson.getString("setAgentStartTime")
          val	setSqlId	= resJson.getString("setSqlId")
          val	setSql	= resJson.getString("setSql")

          // 将数据写入Bean中
          val sqlBean = SqlBean(agentId,startTime,sqlId,sql,hashCode,setAgentId,setAgentStartTime,setSqlId,setSql)

          // 获取当前时间
          getTime()
          // 将数据存储到MySQL
          MysqlUtil.saveTo_sql(sqlBean)
        } else if (topics.equals("AIOPS_ETE_SERVUNKNOWN")){  // unknown  	    AIOPS_ETE_SERVUNKNOWN
          // 解析JSON
          val resJson = JSON.parseObject(valJson)
          // 获取数据

          // 将数据写入Bean中
          val unknownBean = UnknownBean(resJson.toString)

          // 获取当前时间
          getTime()
          // 将数据存储到MySQL
          MysqlUtil.saveTo_unknown(unknownBean)
        }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }


  /*
  * 获取当前时间的函数
  *
  * */
  def getTime(): Unit ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(df.format(new Date()))
  }


  /*
  *  获取对应的topic关系
  * */
  def printTopic(topics : String): Unit ={
    if (topics.contains("AIOPS_ETE_SERVFRAMETOPO")){
      println("即将存储的Topic:\t" + "span")
    } else if (topics.contains("AIOPS_ETE_SERVCHUNKTOPO")){
      println("即将存储的Topic:\t" + "spanChunk")
    } else if (topics.contains("AIOPS_ETE_SERVSTATTOPO")){
      println("即将存储的Topic:\t" + "stat")
    } else if (topics.contains("AIOPS_ETE_SERVAGENTTOPO")){
      println("即将存储的Topic:\t" + "agent")
    } else if (topics.contains("AIOPS_ETE_SERVAPITOPO")){
      println("即将存储的Topic:\t" + "api")
    } else if (topics.contains("AIOPS_ETE_SERVSTRTOPO")){
      println("即将存储的Topic:\t" + "str")
    } else if (topics.contains("AIOPS_ETE_SERVSQLTOPO")){
      println("即将存储的Topic:\t" + "sql")
    } else if (topics.contains("AIOPS_ETE_SERVUNKNOWN")){
      println("即将存储的Topic:\t" + "unknown")
    }
  }

}
