package liuzl.kafkasource

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object ReadBySureOffsetTest {
  val logger = LoggerFactory.getLogger(ReadBySureOffsetTest.getClass)

  def main(args: Array[String]) {
//    val ssc = createContext()
//    //设置打印日志级别
//    Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR)
//    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR)
//    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
//    logger.info("测试从指定offset消费kafka的主程序开始")
//    if (args.length < 1) {
//      System.err.println("Your arguments were " + args.mkString(","))
//      System.exit(1)
//      logger.info("主程序意外退出")
//    }
//    //hdfs://hadoop1:8020/user/root/spark/checkpoint
//    val Array(checkpointDirectory) = args
//    logger.info("checkpoint检查：" + checkpointDirectory)
//    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
//      () => {
//        createContext(checkpointDirectory)
//      })
//    logger.info("streaming开始启动")


    //获取配置
    val brokers = "192.168.165.181:8422,192.168.165.180:8422,192.168.165.179:8422"
    val topics = "AIOPS_ETE_SERVSTRTOPO"

    //默认为5秒
    val split_rdd_time = 8
    // 创建上下文
    val sparkConf = new SparkConf()
      .setAppName("SendSampleKafkaDataToApple")
      .setMaster("local[2]")
      .set("spark.app.id", "streaming_kafka")

    val ssc = new StreamingContext(sparkConf, Seconds(split_rdd_time))
    //    ssc.sparkContext.setLogLevel("WARN")
//    ssc.checkpoint(checkpointDirectory)
        ssc.checkpoint("D://checkpoint")

    // 创建包含brokers和topic的直接kafka流
    val topicsSet: Set[String] = topics.split(",").toSet
    //kafka配置参数
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> "apple_sample",
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
    val offsetList = List((topics, 0, 22753623L),(topics, 1, 327041L))                          //指定topic，partition_no，offset
    //    val fromOffsets = setFromOffsets(offsetList)     //构建参数
    val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.topic, mam.message()) //构建MessageAndMetadata
    //使用高级API从指定的offset开始消费，欲了解详情，
    //请进入"http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$"查看

    println("那你也太没用 了" )

    val test = TopicAndPartition("AIOPS_ETE_SERVSTRTOPO", 0)
    val map1 = Map(test->23.toLong)

    println("到这没有")
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, map1 , messageHandler);

    println(messages)

//    messages.foreachRDD(mess => {
//      //获取offset集合
//      val offsetsList = mess.asInstanceOf[HasOffsetRanges].offsetRanges
//      mess.foreachPartition(lines => {
//        lines.foreach(line => {
//          //          println(line)
//          val o: OffsetRange = offsetsList(TaskContext.get.partitionId)
//          logger.info("++++++++++++++++++++++++++++++此处记录offset+++++++++++++++++++++++++++++++++++++++")
//          logger.info(s"${o.topic}  ${o.partition}  ${o.fromOffset}  ${o.untilOffset}")
//          logger.info("+++++++++++++++++++++++++++++++此处消费数据操作++++++++++++++++++++++++++++++++++++++")
//          logger.info("The kafka  line is " + line)
//        })
//      })
//    })

    messages.foreachRDD { rdd =>
      //lines里存储了当前批次内的所有数据
      println("开始处理了" )
      val lines = rdd.toLocalIterator
      println(lines)
      while (lines.hasNext) {
        val line = lines.next() // 获取每一行的数据
        println(line)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def createContext(checkpointDirectory : String): StreamingContext = {
    //获取配置
    val brokers = "192.168.165.181:8422,192.168.165.180:8422,192.168.165.179:8422"
    val topics = "AIOPS_ETE_SERVSTRTOPO"

    //默认为5秒
    val split_rdd_time = 8
    // 创建上下文
    val sparkConf = new SparkConf()
      .setAppName("SendSampleKafkaDataToApple")
      .setMaster("local[2]")
      .set("spark.app.id", "streaming_kafka")

    val ssc = new StreamingContext(sparkConf, Seconds(split_rdd_time))
//    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkpointDirectory)
//    ssc.checkpoint("D://checkpoint")

    // 创建包含brokers和topic的直接kafka流
    val topicsSet: Set[String] = topics.split(",").toSet
    //kafka配置参数
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> "apple_sample",
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
    val offsetList = List((topics, 0, 22753623L),(topics, 1, 327041L))                          //指定topic，partition_no，offset
//    val fromOffsets = setFromOffsets(offsetList)     //构建参数
    val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.topic, mam.message()) //构建MessageAndMetadata
    //使用高级API从指定的offset开始消费，欲了解详情，
    //请进入"http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$"查看


    val test = TopicAndPartition("AIOPS_ETE_SERVSTRTOPO", 0)
    val map1 = Map(test->23.toLong)

    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, map1 , messageHandler);

    messages.foreachRDD { rdd =>
      //lines里存储了当前批次内的所有数据
      val lines = rdd.toLocalIterator

      while (lines.hasNext) {
        val line = lines.next() // 获取每一行的数据
        println(line)
      }
    }
    /*//数据操作
    messages.foreachRDD(mess => {
      //获取offset集合
      val offsetsList = mess.asInstanceOf[HasOffsetRanges].offsetRanges
      mess.foreachPartition(lines => {
        lines.foreach(line => {
//          println(line)
          val o: OffsetRange = offsetsList(TaskContext.get.partitionId)
          logger.info("++++++++++++++++++++++++++++++此处记录offset+++++++++++++++++++++++++++++++++++++++")
          logger.info(s"${o.topic}  ${o.partition}  ${o.fromOffset}  ${o.untilOffset}")
          logger.info("+++++++++++++++++++++++++++++++此处消费数据操作++++++++++++++++++++++++++++++++++++++")
          logger.info("The kafka  line is " + line)
        })
      })
    })*/
    ssc
  }

//  //构建Map
//  def setFromOffsets(list: List[(String, Int, Long)]): Map[TopicAndPartition, Long] = {
//    var fromOffsets: Map[TopicAndPartition, Long] = Map()
//    for (offset <- list=""  ){
//      val tp = TopicAndPartition(offset(0),offset(1))           // offset位置
//    }
//    fromOffsets
//  }


}
