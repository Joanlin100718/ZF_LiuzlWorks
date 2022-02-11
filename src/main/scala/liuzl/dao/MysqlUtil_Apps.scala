package liuzl.dao

import com.mchange.v2.c3p0.ComboPooledDataSource
import liuzl.pojo._

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.collection.mutable.ListBuffer

object MysqlUtil_Apps {
  
  val c3p0=new ComboPooledDataSource("AppsSource")

  /*
  *   查询对应的offset列表
  *
  * */
  def selectOffsetList (topic:String ) : ListBuffer[Long] ={
    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    // 创建变长的List 存储数据返回
    val list = scala.collection.mutable.ListBuffer(1L)
    list.remove(0)

    try {
      conn=c3p0.getConnection

      // 查询offset
      //SQL语句
      val SQLSentence = "select partition0,partition1,partition2,partition3,partition4,partition5 from historicalOffset where kafkaTopic = \"" + topic  + "\" ;"
      ps = conn.prepareStatement(SQLSentence)
      rs = ps.executeQuery()

      while (rs.next()){

        list.append(rs.getInt("partition0").toLong)
//        list.append(rs.getInt("partition1").toLong)
//        list.append(rs.getInt("partition2").toLong)
//        list.append(rs.getInt("partition3").toLong)
//        list.append(rs.getInt("partition4").toLong)
//        list.append(rs.getInt("partition5").toLong)

      }
      list

    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
        list
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }

  }


  /*
  * V4版本
  * 更新数据库中offset值
  * */
  def updateKafkaOffset(topic:String, partition : Int, offset:Long) : Unit ={
    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection

      // 更新offset 语句
      val updateSQL = "update historicalOffset set partition" + partition + "  = ?  where kafkaTopic =  ? "

      ps = conn.prepareStatement( updateSQL )
      ps.setLong(1, offset)
      ps.setString(2,topic)

//      println("offset更新了")

      ps.executeUpdate()



    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }

  }

  /**
   * 应用使用情况数据
   * 数据存储到对应表中
   */
  def saveTo_application_usage(applicationUsageBean: ApplicationUsageBean ) = {
    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null
    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into application_usage values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,DATE_FORMAT( CURDATE() , '%Y%m%d' ))")

      ps.setString( 	1,	applicationUsageBean.employee_id	)
      ps.setString( 	2,	applicationUsageBean.full_name	)
      ps.setString( 	3,	applicationUsageBean.phone_num	)
      ps.setString( 	4,	applicationUsageBean.device_id	)
      ps.setString( 	5,	applicationUsageBean.package_name	)
      ps.setString( 	6,	applicationUsageBean.app_name	)
      ps.setString( 	7,	applicationUsageBean.version_code	)
      ps.setString( 	8,	applicationUsageBean.version_name	)
      ps.setString( 	9,	applicationUsageBean.TYPE	)
      ps.setString( 	10,	applicationUsageBean.typeDesc	)
      ps.setString( 	11,	applicationUsageBean.tenant_id	)
      ps.setString( 	12,	applicationUsageBean.occur_time	)
      ps.setString( 	13,	applicationUsageBean.upload_time	)
      ps.setString( 	14,	applicationUsageBean.create_time	)


      //      println("************************存一个*************************")
      println(ps.toString)
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }



  /**
   *
   * 流量统计数据
   * 数据存储到对应表中
   */
  def saveTo_application_traffic_usage(applicationTrafficUsageBean: ApplicationTrafficUsageBean ) = {
    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null
    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into application_traffic_usage values(?,?,?,?,?,?,?,?,?,?,?,?,DATE_FORMAT(CURDATE() , '%Y%m%d'))")

      ps.setString( 	1,	applicationTrafficUsageBean.employee_id	)
      ps.setString( 	2,	applicationTrafficUsageBean.full_name	)
      ps.setString( 	3,	applicationTrafficUsageBean.phone_num	)
      ps.setString( 	4,	applicationTrafficUsageBean.device_id	)
      ps.setString( 	5,	applicationTrafficUsageBean.package_name	)
      ps.setString( 	6,	applicationTrafficUsageBean.app_name	)
      ps.setString( 	7,	applicationTrafficUsageBean.wifi_flow	)
      ps.setString( 	8,	applicationTrafficUsageBean.mobile_flow	)
      ps.setString( 	9,	applicationTrafficUsageBean.tenant_id	)
      ps.setString( 	10,	applicationTrafficUsageBean.collect_date	)
      ps.setString( 	11,	applicationTrafficUsageBean.upload_time	)
      ps.setString( 	12,	applicationTrafficUsageBean.create_time	)

      //      println("************************存一个*************************")
      println(ps.toString)
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }




  /**
   *
   * 使用时长数据
   * 数据存储到对应表中
   */
  def saveTo_application_duration(applicationDurationBean: ApplicationDurationBean ) = {
    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null
    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into application_duration values(?,?,?,?,?,?,?,?,?,?,DATE_FORMAT( CURDATE() , '%Y%m%d' ))")

      ps.setString(   1,	applicationDurationBean.employee_id	)
      ps.setString(   2,	applicationDurationBean.package_name	)
      ps.setString(   3,	applicationDurationBean.app_name	)
      ps.setString(   4,	applicationDurationBean.use_duration	)
      ps.setString(   5,	applicationDurationBean.wifi_flow	)
      ps.setString(   6,	applicationDurationBean.mobile_flow	)
      ps.setString(   7,	applicationDurationBean.install_count	)
      ps.setString(   8,	applicationDurationBean.tenant_id	)
      ps.setString(   9,	applicationDurationBean.collect_date	)
      ps.setString(   10,	applicationDurationBean.create_time	)


      //      println("************************存一个*************************")
      println(ps.toString)
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }


}