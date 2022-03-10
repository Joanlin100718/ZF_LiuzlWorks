package liuzl.dao

import com.mchange.v2.c3p0.ComboPooledDataSource
import liuzl.dao.MysqlUtil_SysOamp.getDateFromTimeStamp
import liuzl.pojo._
import liuzl.utils.{JDBC_Druid, JDBC_Druid_SysOamp}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object MysqlUtil_SysOamp_Batch {
  
  val c3p0=new ComboPooledDataSource("SysOampSource")

  /*
  *   查询对应的offset列表
  *
  * */
  def selectOffsetList (topic:String ) : ListBuffer[Long] ={
    var conn:Connection= null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    // 创建变长的List 存储数据返回
    val list = new ListBuffer[Long]
    try {
      conn = JDBC_Druid_SysOamp.getConnection

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
      JDBC_Druid_SysOamp.commit(conn)
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      JDBC_Druid_SysOamp.close(ps,conn,rs)
    }
    list
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

      conn=JDBC_Druid_SysOamp.getConnection

      // 更新offset 语句
      val updateSQL = "update historicalOffset set partition" + partition + "  = ?  where kafkaTopic =  ? "

      ps = conn.prepareStatement( updateSQL )
      ps.setLong(1, offset)
      ps.setString(2, topic)

//      println("offset更新了")

      ps.executeUpdate()

      JDBC_Druid_SysOamp.commit(conn)

    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      JDBC_Druid_SysOamp.close(ps,conn,rs)
    }

  }

  /*
    * 定义 将stat 数据存储到对应的mysql中
    * 因数据量较大，该方式存储过慢
    * */
  def saveStatToMySQL_Batch( lisStatBean: ListBuffer[StatBean]  ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , ChunkBean: ChunkBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      //      conn=c3p0.getConnection
      conn = JDBC_Druid_SysOamp.getConnection

      var batchIndex = 0

      for (statBean <- lisStatBean) {
        // 获取该条语句的时间戳
        val	timeStamp = statBean.timestamp
        // 根据时间戳获取时间
        val conversionDate = getDateFromTimeStamp(timeStamp.toLong)
        // 定义获取对应字段
        val field = statBean.field
        val insertUpdateSql = "INSERT INTO `STAT_"+ conversionDate + "` (" + field + ",first_timestamp) values (?,?) ON DUPLICATE KEY UPDATE " + field + "= VALUES(" + field + ");"
//        println(insertUpdateSql)
        ps = conn.prepareStatement(insertUpdateSql)
        ps.setString(1, statBean.statValues)
        ps.setString(2, timeStamp)

//        println(ps)
        ps.addBatch()
        ps.executeBatch()
//        batchIndex += 1
//        if(batchIndex % 1000 ==0 && batchIndex != 0){
//          ps.executeBatch()
//          ps.clearBatch()
//        }
      }

//      println("本批次：" + batchIndex)
//
//      ps.executeBatch()
      JDBC_Druid_SysOamp.commit(conn)
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      JDBC_Druid_SysOamp.close(ps,conn,rs)
    }
  }

  /**
   * 时间戳(s)转日期
   *
   */
  def getDateFromTimeStamp(timestamp: Long): String = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.format(timestamp)
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