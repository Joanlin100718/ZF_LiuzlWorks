package liuzl.flink


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.util.Properties
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


object KafkaConsumer {
    def main(args: Array[String]): Unit = {

        // 获取执行环境
        val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置检查点时间
        env.enableCheckpointing(5000) //5秒

        // 创建flink-kafka 整合包提供的消费者对象
        //①参:消费的topic  ②参:返回参数的数据类型，利用提供的api  ③参:属性配置对象
        val consumer=new FlinkKafkaConsumer011[String]("topicTest", new SimpleStringSchema(), getKafkaConfig());

        //如果通过scala操作flink,在消费kafka数据时,
        //需要进行导包import org.apache.flink.api.scala._
        val kafkaStream = env.addSource(consumer)

//        println(kafkaStream)
        kafkaStream.print()

        env.execute()

    }


    def getKafkaConfig():Properties={
        val prop:Properties=new Properties()
        // kafka的信息录入
        prop.setProperty("group.id", "test66")        //设定消费者组名
        prop.setProperty("bootstrap.servers", "192.168.135.37:9092")
        prop.setProperty("zookeeper.connect", "192.168.135.37:2181,192.168.135.38:2181,192.168.135.39:2181")
        prop.setProperty("auto.offset.reset", "earliest")
        //        prop.setProperty("auto.offset.reset", "latest")
        //        prop.setProperty("auto.offset.reset", "none")
        prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop
    }


}
