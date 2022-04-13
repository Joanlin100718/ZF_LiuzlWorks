package liuzl.kafkasource

import liuzl.dao.MysqlUtil_SysOamp_Batch
import liuzl.kafkasource.Spark_KafkaTOMySQL_Pass_SingleStat_Batch.{countIndex, listStatBean}

import java.text.SimpleDateFormat

object Test {
  def main(args: Array[String]): Unit = {
    println(getDateFromTimeStamp(1646841651826L))
  }

  def getDateFromTimeStamp(timestamp: Long): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    sdf.format(timestamp)
  }


}
