package liuzl.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.util.Properties
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.flink.streaming.api.scala._

object WordCountByKafka {
    def main(args: Array[String]): Unit = {

        // 获取执行环境
        val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置检查点时间
        env.enableCheckpointing(5000) //5秒

        import org.apache.flink.streaming.api.scala._
        // 设置默认的分区（分区优先级：先找单独设置的分区，若没有就用默认的）
        env.setParallelism(1)

        // 创建flink-kafka 整合包提供的消费者对象
        //①参:消费的topic  ②参:返回参数的数据类型，利用提供的api  ③参:属性配置对象
        val counsumer=new FlinkKafkaConsumer011[String]("topicTest", new SimpleStringSchema(), getKafkaConfig());

        //如果通过scala操作flink,在消费kafka数据时,
        //需要进行导包import org.apache.flink.api.scala._
        val kafkaStream = env.addSource(counsumer)

        // 转换计算
        val result: DataStream[(String, Int)] = kafkaStream.flatMap(_.split(" "))
          .map((_, 1))
          .setParallelism(2) //设置单独的分区
          .keyBy(0) // 分组：必须制定根据哪个字段分组,参数代表当前要分组的字段的下标（另外还有fieldsname)
          .sum(1) // 1代表下标，下标为1的进行累加

        //打印结果到控制台
        result.print()
          .setParallelism(4) //设置单独的分区
        //启动流式处理，如果没有该行代码上面的程序不会运行
        env.execute()

    }


    def getKafkaConfig():Properties={
        val prop:Properties=new Properties()
        // kafka的信息录入
        prop.setProperty("group.id", "WordCount")        //设定消费者组名
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
