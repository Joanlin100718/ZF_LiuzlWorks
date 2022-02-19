package liuzl.dataGovernance

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @program: ZF_Works
 * @description
 * @author: WZP
 * @create: 2022-02-15 11:47
 * */
object demo01 {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test01")
    //创建 SparkSession 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //注意：spark 不是包名，是上下文环境对象名
    //通用的 load 方法读取mysql数据
    val studf: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.153.9:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&nullCatalogMeansCurrent=true&allowPublicKeyRetrieval=true")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root@12#$")
      .option("dbtable", "student")
      .load()


    studf.select("name", "sex", "age")
    val stu = studf.createOrReplaceTempView("stu")
    val sqldf: DataFrame = spark.sql("select * from stu where age>20 and department=2")
    sqldf.show()

    //读取 json 文件 创建 DataFrame {"username": "lisi","age": 18}
    //    val df: DataFrame = spark.read.json("input/test.json")
    //df.show()

    spark.stop()
  }

}
