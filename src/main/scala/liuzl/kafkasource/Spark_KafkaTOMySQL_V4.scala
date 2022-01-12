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

object Spark_KafkaTOMySQL_V4 {
  def main(args: Array[String]): Unit = {

    //获取配置
    val brokers = "192.168.165.181:8422,192.168.165.180:8422,192.168.165.179:8422"
//    val topics = "AIOPS_ETE_SERVSTRTOPO"
    val topics =
//                "AIOPS_ETE_SERVFRAMETOPO,"  +  //span
//                "AIOPS_ETE_SERVCHUNKTOPO,"  +   //spanChunk
      //        "AIOPS_ETE_SERVSTATTOPO,"   +   //stat
//                "AIOPS_ETE_SERVAGENTTOPO," +    //agent
//                "AIOPS_ETE_SERVAPITOPO," +     //api
                "AIOPS_ETE_SERVSTRTOPO,"  +     //str
//                "AIOPS_ETE_SERVUNKNOWN,"   +     // unknown
                "AIOPS_ETE_SERVSQLTOPO"       //sql

    //默认为5秒
    val split_rdd_time = 8
    // 创建上下文
    val sparkConf = new SparkConf()
      .setAppName("SendSampleKafkaDataToApple")
      .setMaster("local[*]")
      .set("spark.app.id", "streaming_kafka")

    val ssc = new StreamingContext(sparkConf, Seconds(split_rdd_time))
    ssc.sparkContext.setLogLevel("WARN")
    // 定义检查点
//    ssc.checkpoint("D://checkpoint")

    // 创建包含brokers和topic的直接kafka流
    val topicsSet: Set[String] = topics.split(",").toSet

    //kafka配置参数
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

    val offsetList = List(("AIOPS_ETE_SERVSTRTOPO", 1, 24L),("AIOPS_ETE_SERVSTRTOPO", 2, 25L),("AIOPS_ETE_SERVSQLTOPO", 0, 28L),("AIOPS_ETE_SERVSQLTOPO", 1, 29L))                          //指定topic，partition_no，offset
    val fromOffsets = setFromOffsets(offsetList)     //构建参数
    val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.topic, mam.offset, mam.message()) //构建MessageAndMetadata
    //使用高级API从指定的offset开始消费，欲了解详情，

    val test = TopicAndPartition("AIOPS_ETE_SERVSTRTOPO", 0)
    val map1 = Map(test->23.toLong)

    println("到这没有")
    val messages: InputDStream[(String, Long ,  String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Long , String)](ssc, kafkaParams, fromOffsets , messageHandler);

    println(messages)

    messages.foreachRDD { rdd =>
      //lines里存储了当前批次内的所有数据
      println("开始处理了" )
      val lines = rdd.toLocalIterator
      println(lines)
      while (lines.hasNext) {
        val line = lines.next() // 获取每一行的数据

        println(line)
        println(line._2)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }


/*  //构建Map
  def setFromOffsets(list: List[(String, Int, Long)]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (offset <- list="" val="" tp="TopicAndPartition(offset._1," offset="" _2="" topic="" fromoffsets="" tp="" -=""> offset._3)           // offset位置
  }
  fromOffsets
}*/
  //构建Map
  def setFromOffsets(lists: List[(String, Int, Long)]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    for (list <- lists){
      val tp = TopicAndPartition(list._1, list._2)
      val topic = list._3.toLong
      fromOffsets+= (tp -> topic)
    }

    fromOffsets
  }


}

