package liuzl.kafkasource

import com.alibaba.fastjson.JSON
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import liuzl.dao.{MysqlUtil_Apps, MysqlUtil_Apps_Batch}
import liuzl.pojo.{AppOperationBean, AppUsageDurationBean, AppUsageFlowBean}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/*
*
*  1. 这个版本通过直接设置开始读取的offset 重启程序时，步入工作较快
*  2. 批量提交一条topic中的数据，数据插入较快，可应对现阶段数据采集
* */


object Spark_KafkaTOMySQL__Pass_Apps_Datch {
  def main(args: Array[String]): Unit = {

    getTime()
    // 定义kafka集群配置信息
    val brokers = "192.168.166.17:8422,192.168.166.16:8422,192.168.166.15:8422"

    // 定义topics列表
    val topics =
                "AppOperation,"  +  //应用使用情况
                "AppUsageFlow," +  //应用使用流量信息
                "AppUsageDuration"  //应用使用时长


    // 定义时间间隔，（默认为5秒）
    val split_rdd_time = 8

    // 创建上下文
    val sparkConf = new SparkConf()
      .setAppName("AppsData")
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

    // kafka 配置参数
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> "liuZL",
      "serializer.class" -> "kafka.serializer.StringEncoder"
      //      "auto.offset.reset" -> "largest"   //自动将偏移重置为最新偏移（默认）
      //      "auto.offset.reset" -> "earliest"  //自动将偏移重置为最早的偏移
      //      "auto.offset.reset" -> "none"      //如果没有为消费者组找到以前的偏移，则向消费者抛出异常
    )

    /**
     * 从指定位置开始读取kakfa数据
     * 注意：由于Exactly  Once的机制，所以任何情况下，数据只会被消费一次！
     *     指定了开始的offset后，将会从上一次Streaming程序停止处，开始读取kafka数据
     */

    // 根据定义的topics列表，构建Map参数
    val fromOffsets = setFromOffsets(topics)     //构建参数

    // 打印Map
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
//        println(valJson)

        if (topic.equals("AppOperation") && valJson.startsWith("{")) { // 应用使用情况
          // 存储应用使用情况
          saveAppOperation(topic , valJson)
        } else if (topic.equals("AppUsageFlow")&& valJson.startsWith("{")) { // 应用使用流量信息
          // 存储应用使用流量信息
          saveAppUsageFlow(topic , valJson)
        } else if (topic.equals("AppUsageDuration")&& valJson.startsWith("{")) { // 应用使用时长
          // 存储应用使用时长
          saveAppUsageDuration(topic , valJson)
        }

        // 更新数据库中的offset
        MysqlUtil_Apps_Batch.updateKafkaOffset(topic,partition,offset)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }


  /*
  *   应用使用情况
  * */
  def saveAppOperation( topics:String, valJson:String ) : Unit = {

    val listAppOperationBean = new ListBuffer[AppOperationBean]
    // 解析JSON 拿到Data中的数据
    val primaryJson = JSON.parseObject(valJson)
    val dataArray = primaryJson.getString("data")

    // 将data中的数据转化为Array
    val jsonArray = JSON.parseArray(dataArray)



    // 遍历Array中的数据进行数据解析
    for (i <- 0.to(jsonArray.size() - 1)) {

      // 将数据中的每一条数据转换为json对象
      val resJson = JSON.parseObject(jsonArray.getJSONObject(i).toString)

      // 将json封装到对应的Bean中
      val	packageName	=	resJson.getString("packageName")
      val	appName	=	resJson.getString("appName")
      val	employeeId	=	resJson.getString("employeeId")
      val	phoneNum	=	resJson.getString("phoneNum")
      val	deviceId	=	resJson.getString("deviceId")
      val	versionCode	=	resJson.getString("versionCode")
      val	versionName	=	resJson.getString("versionName")
      val	`type`	=	resJson.getString("type")
      val	occurTime	=	resJson.getString("occurTime")
      val	uploadTime	=	resJson.getString("uploadTime")
      val	createTime	=	resJson.getString("createTime")


      // 写入Bean中
      val appOperationBean = AppOperationBean(packageName,	appName,	employeeId,	phoneNum, deviceId,	versionCode,	versionName,	`type`,	occurTime,	uploadTime,	createTime)

      // 将appOperationBean 加入List批次列表
      listAppOperationBean.append(appOperationBean)

    }
    // 将数据存储到MySQL
    MysqlUtil_Apps_Batch.saveAppOperationToMySQL(listAppOperationBean)
  }




  /*
  * 应用使用流量信息
  *
  * */

  def saveAppUsageFlow( topics:String, valJson:String ) : Unit = {

    // 定义存储批量数据
    val listAppUsageFlowBean = new ListBuffer[AppUsageFlowBean]

    // 解析JSON 拿到Data中的数据
    val primaryJson = JSON.parseObject(valJson)
    val dataArray = primaryJson.getString("data")

    // 将data中的数据转化为Array
    val jsonArray = JSON.parseArray(dataArray)

    // 遍历Array中的数据进行数据解析
    for (i <- 0.to( jsonArray.size() -1 )) {

      // 将数据中的每一条数据转换为json对象
      val resJson = JSON.parseObject(jsonArray.getJSONObject(i).toString)

      val	employeeId	= resJson.getString("employeeId")
      val	phoneNum	= resJson.getString("phoneNum")
      val	deviceId	= resJson.getString("deviceId")
      val	packageName	= resJson.getString("packageName")
      val	appName	= resJson.getString("appName")
      val	wifiFlow	= resJson.getString("wifiFlow")
      val	mobileFlow	= resJson.getString("mobileFlow")
      val	collectDate	= resJson.getString("collectDate")
      val	uploadTime	= resJson.getString("uploadTime")
      val	createTime	= resJson.getString("createTime")

      // 写入Bean中
      val appUsageFlowBean = AppUsageFlowBean(employeeId,phoneNum,deviceId,packageName,appName,wifiFlow,mobileFlow,collectDate,uploadTime,createTime)

      listAppUsageFlowBean.append(appUsageFlowBean)

    }
    // 将数据存储到MySQL
    MysqlUtil_Apps_Batch.saveAppUsageFlowToMySQL(listAppUsageFlowBean)
  }



  /*
  *
  * 应用使用时长
  *
  * */
  def saveAppUsageDuration( topics:String, valJson:String ) : Unit = {

    val listAppUsageDurationBean = new ListBuffer[AppUsageDurationBean]

    // 解析JSON 拿到Data中的数据
    val primaryJson = JSON.parseObject(valJson)
    val dataArray = primaryJson.getString("data")

    // 将data中的数据转化为Array
    val jsonArray = JSON.parseArray(dataArray)

    // 遍历Array中的数据进行数据解析
    for (i <- 0.to(jsonArray.size() - 1)) {

      // 将数据中的每一条数据转换为json对象
      val resJson = JSON.parseObject(jsonArray.getJSONObject(i).toString)

      // 获取数据

      val	employeeId	= resJson.getString("employeeId")
      val	phoneNum	= resJson.getString("phoneNum")
      val	deviceId	= resJson.getString("deviceId")
      val	packageName	= resJson.getString("packageName")
      val	appName	= resJson.getString("appName")
      val	useDuration	= resJson.getString("useDuration")
      val	openTimes	= resJson.getString("openTimes")
      val	appCount	= resJson.getString("appCount")
      val	collectDate	= resJson.getString("collectDate")
      val	uploadTime	= resJson.getString("uploadTime")
      val	createTime	= resJson.getString("createTime")


      // 写入Bean中
      val appUsageDurationBean = AppUsageDurationBean(employeeId,phoneNum,deviceId,packageName,appName,useDuration,openTimes,appCount,collectDate,uploadTime,createTime)

      listAppUsageDurationBean.append(appUsageDurationBean)
    }
    // 将数据存储到MySQL
    MysqlUtil_Apps_Batch.saveAppUsageDurationToMySQL(listAppUsageDurationBean)
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
      val lists = MysqlUtil_Apps_Batch.selectOffsetList(topic)

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

