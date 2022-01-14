package liuzl.kafkasource

import com.alibaba.fastjson.JSON
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import liuzl.dao.MysqlUtil
import liuzl.kafkasource.Spark_KafkaTOMySQL_STAT.getTime
import liuzl.pojo.{AgentBean, AgentTailBean, ApiBean, SpanBean, SpanChuckBean, SqlBean, StatBean, StrBean, UnknownBean}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

/*
*
* 这个版本通过直接设置开始读取的offset 重启程序时，步入工作较快
*
* */



object Spark_KafkaTOMySQL_V4 {
  def main(args: Array[String]): Unit = {

    // 定义kafka集群配置信息
    val brokers = "192.168.165.181:8422,192.168.165.180:8422,192.168.165.179:8422"

    // 定义topics列表
    val topics =
                "AIOPS_ETE_SERVFRAMETOPO,"  +  //span
                "AIOPS_ETE_SERVCHUNKTOPO,"  +   //spanChunk
//                "AIOPS_ETE_SERVSTATTOPO,"   +   //stat
                "AIOPS_ETE_SERVAGENTTOPO," +    //agent
                "AIOPS_ETE_SERVAPITOPO," +     //api
                "AIOPS_ETE_SERVSTRTOPO,"  +     //str
                "AIOPS_ETE_SERVUNKNOWN,"   +     // unknown
                "AIOPS_ETE_SERVSQLTOPO"       //sql

    // 定义时间间隔，（默认为5秒）
    val split_rdd_time = 8
    // 创建上下文

    val sparkConf = new SparkConf()
      .setAppName("SendSampleKafkaDataToApple")
      .setMaster("local[*]")
      .set("spark.app.id", "streaming_kafka")

    // 定义SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(split_rdd_time))

    // 定义警告级别
    ssc.sparkContext.setLogLevel("WARN")

    // 定义检查点
//    ssc.checkpoint("D://checkpoint")

    // 创建包含brokers和topic的直接kafka流
    val topicsSet: Set[String] = topics.split(",").toSet

    // kafka 配置参数
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> "liuzl",
      "serializer.class" -> "kafka.serializer.StringEncoder"
      //      "auto.offset.reset" -> "largest"   //自动将偏移重置为最新偏移（默认）
      //      "auto.offset.reset" -> "earliest"  //自动将偏移重置为最早的偏移
      //      "auto.offset.reset" -> "none"      //如果没有为消费者组找到以前的偏移，则向消费者抛出异常
    )

    /**
     * 从指定位置开始读取kakfa数据
     * 注意：由于Exactly  Once的机制，所以任何情况下，数据只会被消费一次！
     *      指定了开始的offset后，将会从上一次Streaming程序停止处，开始读取kafka数据
     */

//    val offsetList = List(("AIOPS_ETE_SERVSTRTOPO", 1, 24L),("AIOPS_ETE_SERVSTRTOPO", 2, 25L),("AIOPS_ETE_SERVSQLTOPO", 0, 28L),("AIOPS_ETE_SERVSQLTOPO", 1, 29L))                          //指定topic，partition_no，offset

    // 根据定义的topics列表，构建Map参数
    val fromOffsets = setFromOffsets(topics)     //构建参数

    // 构建返回数据的参数
    val messageHandler = (mam: MessageAndMetadata[String, String]) => ( mam.topic, mam.partition , mam.offset, mam.message()) //构建MessageAndMetadata

    // 获取topic 一个批次中的数据
    val kafkaSource: InputDStream[(String, Int, Long,  String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Int , Long , String)](ssc, kafkaParams, fromOffsets , messageHandler);

    // 数据转换RDD 并进行处理
    kafkaSource.foreachRDD { rdd =>

      //lines里存储了当前批次内的所有数据
      val lines = rdd.toLocalIterator

      // 通过遍历lines获取每一行数据
      while (lines.hasNext) {

        // 获取每一行的数据
        val line = lines.next()

        // 获取数据详情
        val topic     = line._1        // 获取对应的topic
        val partition = line._2        // 获取对应的partition编号
        val offset    = line._3        // 获取对应的offset
        val valJson   = line._4        // 获取每一行中的value值

        println("Topic: " + getTopic(topic) + "\tPartition: " + partition + "\tOffset: " + offset)
        println(valJson)
        // 更新数据库中的offset
        MysqlUtil.updateKafkaOffset(topic,partition,offset)

        // 存储数据
        saveKafkaData(topic , valJson)

      }
    }
    ssc.start()
    ssc.awaitTermination()
  }




  /*
  *  对Kafka中的数据进行存储函数
  *
  * */
  def saveKafkaData(topics:String, valJson:String ) = {

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

      val spanChuckBean = SpanChuckBean(version,agentId,applicationId,agentStartTime,transactionId,spanId,endPoint,serviceType,applicationServiceType,spanEventBoList)

      // 获取当前时间
      getTime()
      // 将数据存储到MySQL
      MysqlUtil.saveTo_spanChuck(spanChuckBean)

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
    } else if (topics.equals("AIOPS_ETE_SERVSTATTOPO")){  // stat      AIOPS_ETE_SERVSTATTOPO

      // 找到第一个 timestamp 下标
      val i = valJson.indexOf("timestamp")

      //通过截取获取 timestamp 对应值 ; 拿到第一个时间戳
      val	timeStamp = valJson.take(i + 24).takeRight(13)

      // 拿到每个topic特有的数据

      // 定义变量，以便得到MySQL表中的字段
      var field = ""

      if ( valJson.contains("\"loadedClassCount\"")) {
        field = "LOADED_CLASS"
      } else if (valJson.contains("\"totalThreadCount\"") ) {
        field = "TOTAL_THREAD"
      }else if ( valJson.contains("\"gcType\"") ) {
        field = "JVM_GC"
      }else if ( valJson.contains("\"gcNewCount\"") ) {
        field = "JVM_GC_DETAILED"
      }else if ( valJson.contains("\"jvmCpuLoad\"") ) {
        field = "CPU_LOAD"
      }else if ( valJson.contains("\"collectInterval\"") ) {
        field = "TRANSACTION"
      }else if ( valJson.contains("\"histogramSchemaType\"") ) {
        field = "ACTIVE_TRACE"
      }else if ( valJson.contains("\"list\"") ) {
        field = "DATASOURCE"
      }else if ( valJson.contains("\"avg\"") ) {
        field = "RESPONSE_TIME"
      }else if ( valJson.contains("\"openFileDescriptorCount\"") ) {
        field = "FILE_DESCRIPTOR"
      }else if ( valJson.contains("\"directCount\"") ) {
        field = "DIRECT_BUFFER"
      }

      // 将数据写入Bean中
      val statBean = StatBean(timeStamp , field , valJson)
      // 获取当前时间
      getTime()
      // 将数据存储到MySQL
      MysqlUtil.saveTo_stat(statBean)
    }
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
  def getTopic(topics : String): String ={
    var topic = ""
    if (topics.contains("AIOPS_ETE_SERVFRAMETOPO")){
      topic =  "span"
    } else if (topics.contains("AIOPS_ETE_SERVCHUNKTOPO")){
      topic =  "spanChunk"
    } else if (topics.contains("AIOPS_ETE_SERVSTATTOPO")){
      topic =  "stat"
    } else if (topics.contains("AIOPS_ETE_SERVAGENTTOPO")){
      topic =   "agent"
    } else if (topics.contains("AIOPS_ETE_SERVAPITOPO")){
      topic =   "api"
    } else if (topics.contains("AIOPS_ETE_SERVSTRTOPO")){
      topic =   "str"
    } else if (topics.contains("AIOPS_ETE_SERVSQLTOPO")){
      topic =   "sql"
    } else if (topics.contains("AIOPS_ETE_SERVUNKNOWN")){
      topic =   "unknown"
    }

    topic
  }


  /*
  * 构建Map
  * */
  def setFromOffsets(topics: String): Map[TopicAndPartition, Long] = {

    // 定义返回值类型
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    // 将topics列表转换为List
    val topicLists = topics.split(",").toList

    // 遍历List 得到数据
    for (topic <- topicLists){

      // 获取各个partition对应的offset
      val lists = MysqlUtil.selectOffsetList(topic)

      // 定义数据下标
      var index = 0

      // 遍历offset列表
      for (list <- lists){

        // 定义返回Map值
        val tp = TopicAndPartition(topic,index)
        val offset = list + 1
        index += 1

        fromOffsets+= (tp -> offset)
      }
    }
    fromOffsets
  }
}

