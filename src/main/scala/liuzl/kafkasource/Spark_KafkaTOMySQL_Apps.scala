package liuzl.kafkasource

import com.alibaba.fastjson.JSON
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import liuzl.dao.{MysqlUtil, MysqlUtil_Apps}
import liuzl.pojo.{ApplicationUsageBean, _}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

/*
*
* 这个版本通过直接设置开始读取的offset 重启程序时，步入工作较快
*
* */


object Spark_KafkaTOMySQL_Apps {
  def main(args: Array[String]): Unit = {

    // 定义kafka集群配置信息
    val brokers = "192.168.165.181:8422,192.168.165.180:8422,192.168.165.179:8422"

    // 定义topics列表
    val topics =
                "application_duration,"  +
                "application_traffic_usage,"  +
                "application_usage"


    // 定义时间间隔，（默认为5秒）
    val split_rdd_time = 8
    // 创建上下文

    val sparkConf = new SparkConf()
      .setAppName("SendSampleKafkaDataToApple")
      .setMaster("local")
      .set("spark.app.id", "streaming_kafka")
//      .set("dfs.client.use.datanode.hostname", "true")
//      .set("dfs.replication", "2")

    // 定义SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(split_rdd_time))

    // 定义警告级别
    ssc.sparkContext.setLogLevel("WARN")


    // 定义检查点
//    ssc.checkpoint("D://checkpoint")

    // 创建包含brokers和topic的直接kafka流
//    val topicsSet: Set[String] = topics.split(",").toSet

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

        println("Topic: " + topic + "\tPartition: " + partition + "\tOffset: " + offset)
        println(valJson)
        // 存储数据
        saveKafkaData(topic , valJson)
        // 更新数据库中的offset
        MysqlUtil_Apps.updateKafkaOffset(topic,partition,offset)

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

    if (topics.equals("application_usage")) { // 应用使用情况
      // 解析JSON
      val resJson = JSON.parseObject(valJson)
      // 获取数据

      val	employee_id	= resJson.getString("employee_id")
      val	full_name	= resJson.getString("full_name")
      val	phone_num	= resJson.getString("phone_num")
      val	device_id	= resJson.getString("device_id")
      val	package_name	= resJson.getString("package_name")
      val	app_name	= resJson.getString("app_name")
      val	version_code	= resJson.getString("version_code")
      val	version_name	= resJson.getString("version_name")
      val	`type`	= resJson.getString("TYPE")
      val	typeDesc	= resJson.getString("typeDesc")
      val	tenant_id	= resJson.getString("tenant_id")
      val	occur_time	= resJson.getString("occur_time")
      val	upload_time	= resJson.getString("upload_time")
      val	create_time	= resJson.getString("create_time")

      // 写入Bean中
      val applicationUsageBean = ApplicationUsageBean(employee_id,full_name,phone_num,device_id,package_name,app_name,version_code,version_name,`type`,typeDesc,tenant_id,occur_time,upload_time,create_time)

      // 获取当前时间
      getTime()

      // 将数据存储到MySQL
      MysqlUtil_Apps.saveTo_application_usage(applicationUsageBean)


    } else if (topics.equals("application_traffic_usage")) { // 流量统计
      // 解析JSON
      val resJson = JSON.parseObject(valJson)
      // 获取数据

      val	employee_id	= resJson.getString("employee_id")
      val	full_name	= resJson.getString("full_name")
      val	phone_num	= resJson.getString("phone_num")
      val	device_id	= resJson.getString("device_id")
      val	package_name	= resJson.getString("package_name")
      val	app_name	= resJson.getString("app_name")
      val	wifi_flow	= resJson.getString("wifi_flow")
      val	mobile_flow	= resJson.getString("mobile_flow")
      val	tenant_id	= resJson.getString("tenant_id")
      val	collect_date	= resJson.getString("collect_date")
      val	upload_time	= resJson.getString("upload_time")
      val	create_time	= resJson.getString("create_time")

      // 写入Bean中
      val applicationTrafficUsageBean = ApplicationTrafficUsageBean(employee_id,full_name,phone_num,device_id,package_name,app_name,wifi_flow,mobile_flow,tenant_id,collect_date,upload_time,create_time)

      // 获取当前时间
      getTime()

      // 将数据存储到MySQL
      MysqlUtil_Apps.saveTo_application_traffic_usage(applicationTrafficUsageBean)
    } else if (topics.equals("application_duration")) { // 使用时长
      // 解析JSON
      val resJson = JSON.parseObject(valJson)
      // 获取数据

      val	employee_id	= resJson.getString("employee_id")
      val	package_name	= resJson.getString("package_name")
      val	app_name	= resJson.getString("app_name")
      val	use_duration	= resJson.getString("use_duration")
      val	wifi_flow	= resJson.getString("wifi_flow")
      val	mobile_flow	= resJson.getString("mobile_flow")
      val	install_count	= resJson.getString("install_count")
      val	tenant_id	= resJson.getString("tenant_id")
      val	collect_date	= resJson.getString("collect_date")
      val	create_time	= resJson.getString("create_time")


      // 写入Bean中
      println(collect_date)
      val applicationDurationBean = ApplicationDurationBean(employee_id,package_name,app_name,use_duration,wifi_flow,mobile_flow,install_count,tenant_id,collect_date,create_time)

      // 获取当前时间
      getTime()

      // 将数据存储到MySQL
      MysqlUtil_Apps.saveTo_application_duration(applicationDurationBean)
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
      val lists = MysqlUtil_Apps.selectOffsetList(topic)

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

