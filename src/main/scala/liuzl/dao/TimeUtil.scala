package liuzl.dao

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object TimeUtil {
  //时间转化为时间戳
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }

  //时间戳转化为时间
  def tranTimeToString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
  //得到本月最后一天的日期
  def getLastDateOfMonth():String={
    val now: Date = new Date();
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    val dateNow = dateFormat.format(now);
    val day =dateNow.substring(0,4)+dateNow.substring(5,7)+dateNow.substring(8,10);
    val year=dateNow.substring(0,4).toInt;
    val month=dateNow.substring(5,7).toInt;
    val cal = Calendar.getInstance();
    cal.set(Calendar.YEAR, year);
    //cal.set(Calendar.MONTH, month-2);//上个月最后一天
    cal.set(Calendar.MONTH, month-1);
    cal.set(Calendar.DAY_OF_MONTH,cal.getActualMaximum(Calendar.DATE));
    new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime())+" 23:59:59"
  }
  //获取当前时间
  def getCurrentTime():String={
    val now: Date = new Date();
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormat.format(now);
  }

  //获取两个日期时间间隔秒数
  def getTimeDifferenceOfTwoTime(startTime:String,endTime:String):Int={
    val l_time=(tranTimeToLong(endTime)-tranTimeToLong(startTime))/1000
    l_time.asInstanceOf[Int]
  }
  def main(args: Array[String]): Unit = {
    println(getCurrentTime())

    println(getTimeDifferenceOfTwoTime(getCurrentTime(),"2020-03-11 14:35:40"))
  }


}
