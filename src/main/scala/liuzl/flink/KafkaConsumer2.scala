package liuzl.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

//import org.apache.flink.api.common.functions.MapFunction
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer



import java.util.Properties
import scala.util.Random


object KafkaConsumer2 {
    def main(args: Array[String]): Unit = {

        // 获取执行环境
        val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置检查点时间
        env.enableCheckpointing(2000)

        import org.apache.flink.api.scala._

        // 创建flink-kafka 整合包提供的消费者对象
        //①参:消费的topic  ②参:返回参数的数据类型，利用提供的api  ③参:属性配置对象
        // [ConsumerRecord[String, String]] 指定数据的返回类型，根据自定义的序列化函数进行返回
        val fkc = new FlinkKafkaConsumer011[ConsumerRecord[String, String]]("topicTest", new MyKafkaDeserializationSchema() , getKafkaConfig);


        /*指定消费位点*/
        /*指定消费位点*/
        val startPartitionsAndOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
        /*这里从topic3 的0分区的第一条开始消费*/
        startPartitionsAndOffsets.put(new KafkaTopicPartition("topicTest", 0), 1L)
        // 对fkc 指定消费起点
        fkc.setStartFromSpecificOffsets(startPartitionsAndOffsets)

        /*指定source数据源*/
        //如果通过scala操作flink,在消费kafka数据时,
        //需要进行导包import org.apache.flink.api.scala._
        val source : DataStream[ConsumerRecord[String, String]] = env.addSource(fkc)

        println(source)

        // 对返回值进行处理

        import org.apache.flink.api.scala._

        val keyValue = source.map(new MapFunction[ConsumerRecord[String, String],String] {
            override def map(message: ConsumerRecord[String, String]): String = {
                "key" + message.key + "  value:" + message.value
            }
        })

        // 打印数据流中的内容
        keyValue.print().setParallelism(1)

        // 执行数据流
        env.execute("MapDemoByScala")

    }


    def getKafkaConfig():Properties={
        val prop:Properties=new Properties()
        // kafka的信息录入
        prop.setProperty("bootstrap.servers", "192.168.135.37:9092")
        prop.setProperty("group.id", "topic_4")        //设定消费者组名
//        prop.setProperty("zookeeper.connect", "192.168.135.37:2181,192.168.135.38:2181,192.168.135.39:2181")
//        prop.setProperty("auto.offset.reset", "earliest")
        prop.setProperty("auto.offset.reset", "latest")
//        prop.setProperty("auto.offset.reset", "none")
        prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop
    }


    //随机产生单词//随机产生单词
     def randomWords() = {
         val random = new Random()
         //产生随机单词
        val wordArray = Array("good", "cheer", "strive", "optimistic", "hello", "word", "teacher", "student", "book", "genius", "handsome", "beautiful", "health", "happy", "world", "computer", "english", "json", "eat", "me", "reset", "center", "blue", "green", "yellow")
        val line = random.nextInt(25) //定义随机数区间[0,25]
         wordArray(line)
    }


}
