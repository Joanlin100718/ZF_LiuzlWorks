package liuzl.kafkasource

import com.alibaba.fastjson.JSON
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import liuzl.dao.{MysqlUtil, MysqlUtil_User}
import liuzl.kafkasource.Spark_KafkaTOMySQL_V4.getTime
import liuzl.pojo._
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


object Spark_KafkaTOMySQL_OnlyUser {
  def main(args: Array[String]): Unit = {

    // 定义kafka集群配置信息
    val brokers = "192.168.165.203:9092"

    // 定义topics列表
    val topics =
                "sys_user_add,"  +
                "sys_user_del,"  +
                "sys_user_update,"  +
                "user"

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

//    val offsetList = List(("sys_user_del", 0,0L),("sys_user_add", 0, 0L),("sys_user_update", 0, 0L),("user", 0, 0L))                          //指定topic，partition_no，offset

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

        val trueOrFalse = false
        // 存储数据
        val resultTrueOrFalse  = saveKafkaData(topic , valJson ,trueOrFalse)
        if (resultTrueOrFalse) {
          // 更新数据库中的offset
          MysqlUtil_User.updateKafkaOffset(topic,partition,offset)
        }
        println("***************************下一波开始了******************************")
        println()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def saveKafkaData(topics:String, valJson:String ,trueOrFalse : Boolean ):Boolean = {

    var TOF = trueOrFalse ;

    if (topics.equals("sys_user_add")){   // sys_user_add
      // 解析JSON
      val resJson = JSON.parseObject(valJson)
      // 获取数据
      val	userId      	=	resJson.getString("userId")
      val	userName    	=	resJson.getString("userName")
      val	admin       	=	resJson.getBoolean("admin")
      val	mobile      	=	resJson.getString("mobile")
      val	nickName    	=	resJson.getString("nickName")
      val	params      	=	resJson.getString("params")
      val	password    	=	resJson.getString("password")
      val	phonenumber 	=	resJson.getString("phonenumber")
      val	userType    	=	resJson.getString("userType")


      // 写入Bean中
      val userBean = UserBean(userId, userName, admin, mobile, nickName, params, password, phonenumber,userType)

      // 获取当前时间
      getTime()

      // 将数据存储到MySQL
      MysqlUtil_User.saveTo_Jq_Register( userBean )

      TOF = true
    } else if (topics.equals("sys_user_del")){   // sys_user_del

      // 对数据进行处理，得到userId
      val delUserId =  valJson.drop(1).dropRight(1)

      // 获取当前时间
      getTime()
      // 删除用户信息
      MysqlUtil_User.deleteTo_Jq_Register( delUserId )
      TOF = true
    } else if (topics.equals("sys_user_update")){   // sys_user_add
      // 解析JSON
      val resJson = JSON.parseObject(valJson)
      // 获取数据
      val	userId      	=	resJson.getString("userId")
      val	userName    	=	resJson.getString("userName")
      val	admin       	=	resJson.getBoolean("admin")
      val	mobile      	=	resJson.getString("mobile")
      val	nickName    	=	resJson.getString("nickName")
      val	params      	=	resJson.getString("params")
      val	password    	=	resJson.getString("password")
      val	phonenumber 	=	resJson.getString("phonenumber")
      val	userType    	=	resJson.getString("userType")


      // 写入Bean中
      val userBean = UserBean(userId, userName, admin, mobile, nickName, params, password, phonenumber ,userType)

      // 获取当前时间
      getTime()

      // 将数据存储到MySQL
      MysqlUtil_User.updateTo_Jq_Register( userBean )

      TOF = true
    }
    TOF
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
      val lists = MysqlUtil_User.selectOffsetList(topic)

      // 定义数据下标
      var index = 0

      // 遍历offset列表
      for (list <- lists){

        // 定义返回Map值
        val tp = TopicAndPartition(topic,index)

        var offset = 0L

        // 获取offset偏移量
        if (list == 0) {
          offset = list
        } else {
          offset = list + 1
        }
        index += 1

        fromOffsets+= (tp -> offset)
      }
    }
    fromOffsets
  }
}

