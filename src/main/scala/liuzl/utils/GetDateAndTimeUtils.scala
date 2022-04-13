package liuzl.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object GetDateAndTimeUtils {

    // 日期时间 转化 为时间戳
    def tranDateAndTimeToTimestamp(tm: String): Long = {
        val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dt = fm.parse(tm)
        val tim: Long = dt.getTime()
        // 返回值
        tim
    }

    // 时间戳 转化为 日期时间
    def tranTimestampToDateAndTime(tm: String): String = {
        val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val tim = fm.format(new Date(tm.toLong))
        tim
    }

    // 时间戳 转化为 日期
    def tranTimestampToDate0(tm: String): String = {
        val fm = new SimpleDateFormat("yyyy-MM-dd")
        val tim = fm.format(new Date(tm.toLong))
        tim
    }

    // 时间戳 转化为 日期
    def tranTimestampToDate1(tm: String): String = {
        val fm = new SimpleDateFormat("yyyyMMdd")
        val tim = fm.format(new Date(tm.toLong))
        tim
    }

    // 时间戳 转化为 日期
    def tranTimestampToDate2(tm: String): String = {
        val fm = new SimpleDateFormat("yyyy/MM/dd")
        val tim = fm.format(new Date(tm.toLong))
        tim
    }


    // 时间戳 转化为 时间
    def tranTimestampToTime(tm: String): String = {
        val fm = new SimpleDateFormat("HH:mm:ss")
        val tim = fm.format(new Date(tm.toLong))
        tim
    }

    // 获取当前时间戳
    def getNowTimestamp(): Long = {
        val now: Date = new Date();
        val timeStamp = now.getTime
        timeStamp
    }


    //获取今天 日期+时间
    def getCurrentDateAndTime0(): String = {
        val now: Date = new Date();
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.format(now);
    }


    // 获取今天日期
    def getCurrentDate0(): String = {
        val now: Date = new Date();
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.format(now);
    }


    // 获取今天日期
    def getCurrentDate1(): String = {
        val now: Date = new Date();
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.format(now);
    }


    // 获取今天日期
    def getCurrentDate2(): String = {
        val now: Date = new Date();
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd");
        dateFormat.format(now);
    }


    // 获取昨天的日期
    def getYesterdayDate0():String= {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        val yesterday = dateFormat.format(cal.getTime())
        yesterday
    }


    // 获取昨天的日期
    def getYesterdayDate1():String= {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        val yesterday = dateFormat.format(cal.getTime())
        yesterday
    }

    // 获取昨天的日期
    def getYesterdayDate2():String= {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        val yesterday = dateFormat.format(cal.getTime())
        yesterday
    }


    // 获取明天的日期
    def getTomorrowDate0():String= {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, +1)
        val tomorrow = dateFormat.format(cal.getTime())
        tomorrow
    }


    // 获取明天的日期
    def getTomorrowDate1():String= {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, +1)
        val tomorrow = dateFormat.format(cal.getTime())
        tomorrow
    }


    // 获取明天的日期
    def getTomorrowDate2():String= {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, +1)
        val tomorrow = dateFormat.format(cal.getTime())
        tomorrow
    }

    // 本月的第一天
    def getNowMonthStart():String={
        val cal:Calendar =Calendar.getInstance();
        val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        cal.set(Calendar.DATE, 1)
        val period=df.format(cal.getTime())//本月第一天
        period
    }



    // 本月的最后一天
    def getNowMonthEnd():String={
        val cal:Calendar =Calendar.getInstance();
        val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        cal.set(Calendar.DATE, 1)
        cal.roll(Calendar.DATE,-1)
        val period=df.format(cal.getTime())//本月最后一天
        period
    }



    //获取两个日期时间间隔秒数
    def getTimeDifferenceOfTwoTime( endTime: String ,startTime: String): Int = {
        val l_time = (tranDateAndTimeToTimestamp(endTime) - tranDateAndTimeToTimestamp(startTime)) / 1000
        l_time.asInstanceOf[Int]
    }


}
