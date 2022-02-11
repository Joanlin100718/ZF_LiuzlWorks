package liuzl.kafkasource

import com.alibaba.fastjson.JSON
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import liuzl.dao.MysqlUtil_Apps
import liuzl.pojo._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

/*
*
* 这个版本通过直接设置开始读取的offset 重启程序时，步入工作较快
*
* */


object Spark_KafkaTOMySQL_SQLContext {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("liuzl")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val dataDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://192.168.153.10:3306/topics?useSSL=false&useUnicode=true&amp;characterEncoding=utf8&amp;useSSL=false&amp;serverTimezone=Asia/Shanghai&amp;nullCatalogMeansCurrent=true&amp;allowPublicKeyRetrieval=true")
      .option("dbtable","topic_sql")
      .option("user","root")
      .option("password","root@12#$")
      .load()




    dataDF.createOrReplaceTempView("topic_sql")
    val sql = "select * from topic_sql "
    spark.sql(sql).show()
    spark.stop()


//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "root@12#$")
//    val url = "jdbc:mysql://192.168.153.9:3306/application_market?useSSL=false&useUnicode=true&amp;characterEncoding=utf8&amp;useSSL=false&amp;serverTimezone=Asia/Shanghai&amp;nullCatalogMeansCurrent=true&amp;allowPublicKeyRetrieval=true"
//
//    val dataFrame = spark.read.jdbc(url,"test",prop).select("values").show()

  }
}

