package liuzl.kafkasource

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import liuzl.dao.{MysqlUtil_SysOamp, MysqlUtil_SysOamp_Batch}
import liuzl.pojo._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/*
*
* 读取topic中的stat，单独对stat进行数据采集操作。
*
*
*
* */


object Spark_KafkaTOMySQL_Pass_SingleStat_Batch {

  var currentDate = ""
  var countIndex = 20
  private val listStatBean = new ListBuffer[StatBean]


  def main(args: Array[String]): Unit = {

    // 定义kafka集群配置信息
    val brokers = "192.168.166.17:8422,192.168.166.16:8422,192.168.166.15:8422"

    // 定义topics列表
    val topics = "AIOPS_ETE_SERVSTATTOPO"     //stat

    // 定义时间间隔，（默认为5秒）
    val split_rdd_time = 8
    // 创建上下文

    val sparkConf = new SparkConf()
      .setAppName("collectionStatBatch")
      .setMaster("yarn")
      .set("spark.app.id", "streaming_kafka")
//      .set("mapreduce.output.fileoutputformat.outputdir", "/data01/SparkJar/tmp")

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
      "group.id" -> "liuZL_Stat",
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
//    var fromOffsets1: Map[TopicAndPartition, Long] = Map()
//    // 定义返回Map值
//    val tp = TopicAndPartition("AIOPS_ETE_SERVSTATTOPO",2)
//
//    fromOffsets1 += (tp -> 11793000L)
//    println(fromOffsets1)


    // 根据定义的topics列表，构建Map参数
    val fromOffsets = setFromOffsets(topics)     //构建参数
    println(fromOffsets)
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

//        println("Topic: " + getTopic(topic) + "\tPartition: " + partition + "\tOffset: " + offset)
//        println(valJson)
        // 存储数据
        saveKafkaData(topic , valJson)
        // 更新数据库中的offset
        MysqlUtil_SysOamp_Batch.updateKafkaOffset(topic,partition,offset)

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

    if (topics.equals("AIOPS_ETE_SERVSTATTOPO")){  // stat      AIOPS_ETE_SERVSTATTOPO

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
//      getTime()
      listStatBean.append(statBean)
      if (countIndex == 0){
        // 将数据存储到MySQL
        MysqlUtil_SysOamp_Batch.saveStatToMySQL_Batch(listStatBean)
        listStatBean.clear()
        countIndex = 20
      } else {
        countIndex -= 1
      }
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
      val lists = MysqlUtil_SysOamp_Batch.selectOffsetList(topic)

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

