package liuzl.dao

import com.mchange.v2.c3p0.ComboPooledDataSource
import liuzl.pojo._
import liuzl.utils.JDBC_Druid

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.collection.mutable.ListBuffer

object MysqlUtil_Apps {
  
  val c3p0=new ComboPooledDataSource("AppsSource")

  private var conn:Connection=JDBC_Druid.getConnection
  private var ps:PreparedStatement=_
  private var rs:ResultSet=_
  private var count = 8


  /*
  *   查询对应的offset列表
  *
  * */
  def selectOffsetList (topic:String ) : ListBuffer[Long] ={


    // 创建变长的List 存储数据返回
    val list = scala.collection.mutable.ListBuffer(1L)
    list.remove(0)

    try {

      // 查询offset
      //SQL语句
      val SQLSentence = "select partition0,partition1,partition2,partition3,partition4,partition5 from historicalOffset where topic = \"" + topic  + "\" ;"
      ps = conn.prepareStatement(SQLSentence)
      rs = ps.executeQuery()

      while (rs.next()){

        list.append(rs.getInt("partition0").toLong)
        list.append(rs.getInt("partition1").toLong)
        list.append(rs.getInt("partition2").toLong)
        list.append(rs.getInt("partition3").toLong)
        list.append(rs.getInt("partition4").toLong)
        list.append(rs.getInt("partition5").toLong)
      }
      list

    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
        list
    }finally {

    }

  }


  /*
  * V4版本
  * 更新数据库中offset值
  * */
  def updateKafkaOffset(topic:String, partition : Int, offset:Long) : Unit ={

    try {

      // 更新offset 语句
      val updateSQL = "update historicalOffset set partition" + partition + "  = ?  where topic =  ? "

      ps = conn.prepareStatement( updateSQL )
      ps.setLong(1, offset)
      ps.setString(2,topic)

//      println("offset更新了")

      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      count -= 1
      if (count == 0 ){
        JDBC_Druid.commit(conn)
        JDBC_Druid.close(ps , conn, rs)
        conn=JDBC_Druid.getConnection
        count = 8
      }
    }

  }

  /**
   * 应用使用情况数据
   * 数据存储到对应表中
   */
  def saveAppOperationToMySQL(appOperationBean: AppOperationBean ) = {

    try {

      //SQL语句
//      ps=conn.prepareStatement("insert into app_operation (packageName,appName,employeeId,phoneNum, deviceId,versionCode,versionName,`type`,occurTime,uploadTime,createTime,warehousingTime) values(?,?,?,?,?,?,?,?,?,?,?,DATE_FORMAT( CURDATE() , '%Y%m%d' ))")
      ps=conn.prepareStatement("insert into app_operation (packageName,appName,employeeId,phoneNum, deviceId,versionCode,versionName,`type`,occurTime,uploadTime,createTime,warehousingTime) values(?,?,?,?,?,?,?,?,?,?,?,NOW())")

      ps.setString( 	1,	appOperationBean.packageName	)
      ps.setString( 	2,	appOperationBean.appName)
      ps.setString( 	3,	appOperationBean.employeeId)
      ps.setString( 	4,	appOperationBean.phoneNum)
      ps.setString( 	5,	appOperationBean.deviceId	)
      ps.setString( 	6,	appOperationBean.versionCode)
      ps.setString( 	7,	appOperationBean.versionName	)
      ps.setString( 	8,	appOperationBean.`type`	)
      ps.setString( 	9,	appOperationBean.occurTime)
      ps.setString( 	10,	appOperationBean.uploadTime)
      ps.setString( 	11,	appOperationBean.createTime)

      println("************************存一个*************************")
      ps.executeUpdate()

    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      count -= 1
      if (count == 0 ){
        JDBC_Druid.commit(conn)
        JDBC_Druid.close(ps , conn, rs)
        conn=JDBC_Druid.getConnection
        count = 8
      }
    }
  }



  /**
   *
   * 流量统计数据
   * 数据存储到对应表中
   */
  def saveAppUsageFlowToMySQL(appUsageFlowBean: AppUsageFlowBean ) = {

    try {

      //SQL语句
      ps=conn.prepareStatement("insert into app_usage_flow (employeeId,phoneNum,deviceId,packageName,appName,wifiFlow,mobileFlow,collectDate,uploadTime,createTime,warehousingTime) values(?,?,?,?,?,?,?,?,?,?,NOW())")

      ps.setString( 	1,	appUsageFlowBean.employeeId  	)
      ps.setString( 	2,	appUsageFlowBean.phoneNum    )
      ps.setString( 	3,	appUsageFlowBean.deviceId    )
      ps.setString( 	4,	appUsageFlowBean.packageName )
      ps.setString( 	5,	appUsageFlowBean.appName     	)
      ps.setString( 	6,	appUsageFlowBean.wifiFlow    )
      ps.setString( 	7,	appUsageFlowBean.mobileFlow  )
      ps.setString( 	8,	appUsageFlowBean.collectDate 	)
      ps.setString( 	9,	appUsageFlowBean.uploadTime  )
      ps.setString( 	10,	appUsageFlowBean.createTime  	)

      println("************************存一个*************************")
      ps.executeUpdate()
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      count -= 1
      if (count == 0 ){
        JDBC_Druid.commit(conn)
        JDBC_Druid.close(ps , conn, rs)
        conn=JDBC_Druid.getConnection
        count = 8
      }
    }
  }




  /**
   *
   * 使用时长数据
   * 数据存储到对应表中
   */
  def saveAppUsageDurationToMySQL(appUsageDurationBean: AppUsageDurationBean ) = {

    try {

      //SQL语句
      ps=conn.prepareStatement("insert into app_usage_duration (employeeId,phoneNum,deviceId,packageName,appName,useDuration,openTimes,appCount,collectDate,uploadTime,createTime,warehousingTime) values(?,?,?,?,?,?,?,?,?,?,?,NOW())")

      ps.setString(   1,	appUsageDurationBean.employeeId)
      ps.setString(   2,	appUsageDurationBean.phoneNum)
      ps.setString(   3,	appUsageDurationBean.deviceId)
      ps.setString(   4,	appUsageDurationBean.packageName)
      ps.setString(   5,	appUsageDurationBean.appName)
      ps.setString(   6,	appUsageDurationBean.useDuration)
      ps.setString(   7,	appUsageDurationBean.openTimes)
      ps.setString(   8,	appUsageDurationBean.appCount)
      ps.setString(   9,	appUsageDurationBean.collectDate)
      ps.setString(   10,	appUsageDurationBean.uploadTime)
      ps.setString(   11,	appUsageDurationBean.createTime)


      println("************************存一个*************************")
      ps.executeUpdate()

    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      count -= 1
      if (count == 0 ){
        JDBC_Druid.commit(conn)
        JDBC_Druid.close(ps , conn, rs)
        conn=JDBC_Druid.getConnection
        count = 8
      }
    }
  }


}