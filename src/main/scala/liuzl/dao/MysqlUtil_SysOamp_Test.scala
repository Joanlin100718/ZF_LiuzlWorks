package liuzl.dao

import com.mchange.v2.c3p0.ComboPooledDataSource
import liuzl.pojo._

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object MysqlUtil_SysOamp_Test {
  
  val c3p0=new ComboPooledDataSource("SysOampSource")

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
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }

  }


  /*
  *
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
      ps.setString(2, topic)

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


  /*
  * 定义 将stat 数据存储到对应的mysql中
  *
  * */
  def saveTo_stat( statBean: StatBean  ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , ChunkBean: ChunkBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var ps1:PreparedStatement=null
    var ps2:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      val	timeStamp = statBean.timestamp
      // 定义获取对应字段
      val field = statBean.field
      // 定义查询语句
      val SQLSentence = "select first_timestamp from stat where first_timestamp=" + timeStamp + ";"
      ps = conn.prepareStatement(SQLSentence)
      rs = ps.executeQuery()

      if (rs.next()) {
          // 更新语句
          val updateSQL = "update stat set " + field + "='" + statBean.statValues + "' where first_timestamp =  ? "
          ps1 = conn.prepareStatement(updateSQL)
          ps1.setString(1, timeStamp)
          ps1.executeUpdate()
          println("************************更新一个*************************")
        }
        else {
          // 插入语句
          val insertSQL = "insert into `stat` (" + field + ",first_timestamp,importDate) values (?,?,NOW())"
          ps2 = conn.prepareStatement(insertSQL)
          ps2.setString(1, statBean.statValues)
          ps2.setString(2, timeStamp)
          println("************************存一个*************************")
          ps2.executeUpdate()}


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error

    }finally {
      if(ps!=null)ps.close
      if(ps1!=null)ps.close
      if(ps2!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }

  /*
  * 定义 将stat 数据存储到对应的mysql中
  *
  * */
  def saveTo_singleStat( statBean: StatBean  ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , ChunkBean: ChunkBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null

    try {
      conn=c3p0.getConnection

      val	timeStamp = statBean.timestamp
      // 定义获取对应字段
      val field = statBean.field

      val insertSQL = "insert into `STAT_" + field + "_" + getDateFromTimeStamp(timeStamp.toLong)  +  "` values (?,?,DATE_FORMAT(CURDATE() , '%Y%m%d'))"

      println(insertSQL)
      ps = conn.prepareStatement(insertSQL)
      ps.setString(1, statBean.statValues)
      ps.setString(2, timeStamp)
      println("************************存一个*************************")
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error

    }finally {
      if(ps!=null)ps.close
      if(conn!=null)conn.close
    }
  }

  /**
   * 时间戳(s)转日期
   *
   */
  def getDateFromTimeStamp(timestamp: Long): String = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.format(1645578044733L)
  }


  /*
  *
  *  获取日期的函数
  * */
  def getTime(): String ={
    val df = new SimpleDateFormat("yyyyMMdd")
    val res = df.format(new Date())
    res
  }

  def getTableName(topics : String): String ={
    var res = ""
    if (topics.contains("AIOPS_ETE_SERVFRAMETOPO")){
      res = "span"
    } else if (topics.contains("AIOPS_ETE_SERVCHUNKTOPO")){
      res = "spanChunk"
    } else if (topics.contains("AIOPS_ETE_SERVSTATTOPO")){
      res = "stat"
    } else if (topics.contains("AIOPS_ETE_SERVAGENTTOPO")){
      res = "agent"
    } else if (topics.contains("AIOPS_ETE_SERVAPITOPO")){
      res = "api"
    } else if (topics.contains("AIOPS_ETE_SERVSTRTOPO")){
      res = "str"
    } else if (topics.contains("AIOPS_ETE_SERVSQLTOPO")){
      res = "sql"
    } else if (topics.contains("AIOPS_ETE_SERVUNKNOWN")){
      res = "unknown"
    }
    res
  }

}