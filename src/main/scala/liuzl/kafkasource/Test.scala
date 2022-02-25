package liuzl.kafkasource

import java.text.SimpleDateFormat
import java.util
import java.util.Date

object Test {
  def main(args: Array[String]): Unit = {


    val tableList  = scala.collection.mutable.ListBuffer[String]()
    tableList.append("ods_sys_oamp_agent_di")
    tableList.append("ods_sys_oamp_agent_tail_di")
    tableList.append("ods_sys_oamp_api_di")
    tableList.append("ods_sys_oamp_span_di")
    tableList.append("ods_sys_oamp_spanchunk_di")
    tableList.append("ods_sys_oamp_sql_di")
    tableList.append("ods_sys_oamp_stat_di")
    tableList.append("ods_sys_oamp_str_di")
    tableList.append("ods_sys_oamp_unknown_di")






    var importdate = 20220219

    for (i <- tableList){
      var importdate = 20220219
      for (j <- 1 to 10) {
        val tableName = i
        val str = "ALTER TABLE " + tableName + " ADD PARTITION (importdate='" + importdate + "') LOCATION '/apps/hive/warehouse/sys_oamp_db.db/" + tableName + "/importdate=" + importdate + "';"
        importdate += 1
        println(str)
      }
    }


    println()

    for (i <- tableList){
      val tableName = i
      val str = "drop TABLE " + tableName + ";"
      println(str)
    }

    println()


    for (i <- tableList){
      val tableName = i
      val str = "hadoop fs -rm -r /apps/hive/warehouse/sys_oamp_db.db/" + tableName
      println(str)
    }







//    println(getTime())
//    println(getDateFromTimeStamp(1645663003476L))
//    println(getDateFromTimeStamp(1645526263476L))

  }


  /**
   * 时间戳(s)转日期
   *
   */
  def getDateFromTimeStamp(timestamp: Long): String = {
//    val sdf = new SimpleDateFormat("yyyyMMdd hhMMss")
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.format(timestamp)
  }


  def getTime(): String ={
    val df = new SimpleDateFormat("yyyyMMdd")

    val res = df.format(new Date())
    res
  }


  def singleStat(): Unit ={
    val tableList  = scala.collection.mutable.ListBuffer[String]()
    tableList.append("loaded_class")
    tableList.append("total_thread")
    tableList.append("jvm_gc")
    tableList.append("jvm_gc_detailed")
    tableList.append("cpu_load" )
    tableList.append("transaction" )
    tableList.append("active_trace" )
    tableList.append("datasource")
    tableList.append("response_time" )
    tableList.append("file_descriptor")
    tableList.append("direct_buffer" )






    val importdate = "20220223"

    for (i <- tableList){
      val tableName = "ods_sys_oamp_stat_" + i + "_di"
      val str = "ALTER TABLE " + tableName + " ADD PARTITION (importdate='" + importdate + "') LOCATION '/apps/hive/warehouse/sys_oamp_db.db/" + tableName  + "/importdate=" + importdate + "';"
      println(str)
    }


    println()

    for (i <- tableList){
      val tableName = "ods_sys_oamp_stat_" + i + "_di"
      val str = "drop TABLE " + tableName + ";"
      println(str)
    }

    println()


    for (i <- tableList){
      val tableName = "ods_sys_oamp_stat_" + i + "_di"
      val str = "hadoop fs -rm -r /apps/hive/warehouse/sys_oamp_db.db/" + tableName
      println(str)
    }


    println()

    "STAT_ACTIVE_TRACE_20220223"
    for (i <- tableList){
      val tableName = "STAT_" + i.toUpperCase + "_20220224"
      val str = "DROP TABLE IF EXISTS `" + tableName + "`;"
      val tableName1 = "STAT_" + i.toUpperCase + "_20220225"
      val str1 = "DROP TABLE IF EXISTS `" + tableName1 + "`;"
      val tableName2 = "STAT_" + i.toUpperCase + "_20220226"
      val str2 = "DROP TABLE IF EXISTS `" + tableName2 + "`;"
      println(str)
      println(str1)
      println(str2)
    }




    //    println(getTime())
    //    println(getDateFromTimeStamp(1645663003476L))
    //    println(getDateFromTimeStamp(1645526263476L))
  }

}
