package liuzl.flink

import jodd.util.ThreadUtil.sleep
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.concurrent.duration.{MINUTES, SECONDS}
import scala.util.Random


object KafkaProducer {
    def main(args: Array[String]): Unit = {
        val kafkaProducer :KafkaProducer[String , String] = new KafkaProducer[String,String](getKafkaConfig)
        while (true) {
            val sinValues = randomWords() // 得到输出的单词

            // 定义Sink对
            val sink = kafkaProducer.send(new ProducerRecord[String, String]("topicTest", sinValues))

            println( "输入:\t"  + sinValues)
            sink.get()

            sleep(1000) // 一秒发一条数据
        }
    }
    //随机产生单词//随机产生单词
    def randomWords() = {
        val random = new Random()
        //产生随机单词
        val wordArray = Array("good", "cheer", "strive", "optimistic", "hello", "word", "teacher", "student", "book", "genius", "handsome", "beautiful", "health", "happy", "world", "computer", "english", "json", "eat", "me", "reset", "center", "blue", "green", "yellow")
        val line = random.nextInt(25) //定义随机数区间[0,25]
        wordArray(line)
    }

    def getKafkaConfig():Properties={
        val prop:Properties=new Properties()
        // kafka的配置信息信息录入
        prop.setProperty("bootstrap.servers", "192.168.135.37:9092")
        prop.setProperty("transaction.timeout.ms",60000*15+"")
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop
    }


}
