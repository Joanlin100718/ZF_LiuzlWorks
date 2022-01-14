package liuzl.dao


import com.alibaba.fastjson.JSON
import com.mchange.v2.c3p0.ComboPooledDataSource
import liuzl.pojo.{AgentBean, AgentTailBean, ApiBean, SpanBean, SpanChuckBean, SqlBean, StatBean, StrBean, UnknownBean}

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer

object MysqlUtil {
  
  val c3p0=new ComboPooledDataSource

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

      println("offset更新了")

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
  *  V3  版本中使用
  *
  * */
  def selectAndUpdateOffset(topic:String , offset:Long , partition : Int) : Boolean ={
    var conn:Connection=null
    var ps:PreparedStatement=null
    var ps1:PreparedStatement=null
    var rs:ResultSet=null
    var res:Boolean = false
    try {
      conn=c3p0.getConnection

      // 查询offset
      //SQL语句
      val SQLSentence = "select partition" + partition + "  from historicalOffset where kafkaTopic = \"" + topic  + "\" ;"
      ps = conn.prepareStatement(SQLSentence)
      rs = ps.executeQuery()

      while (rs.next()){

        val tableOffset = rs.getString("partition" + partition)

        if (offset > tableOffset.toInt){

          // 更新offset
          val updateSQL = "update historicalOffset set partition" + partition + "  = ?  where kafkaTopic =  ? "

          ps1 = conn.prepareStatement( updateSQL )
          ps1.setLong(1, offset)
          ps1.setString(2,topic)

          ps1.executeUpdate()

          res = true
        } else {
          res = false
        }
      }
      res

    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
      res
    }finally {
      if(ps!=null)ps.close
      if(ps1!=null)ps1.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }

  }

  
  /**
   * 数据存储到对应表中
   */
  def saveTo_agent(agentBean: AgentBean ) = {
    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null
    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into agent values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())")
      ps.setString( 1  ,	agentBean.hostName	)
      ps.setString( 2  ,	agentBean.ip	)
      ps.setString( 3  ,	agentBean.ports	)
      ps.setString( 4  ,	agentBean.agentId	)
      ps.setString( 5  ,	agentBean.applicationName	)
      ps.setString( 6  ,	agentBean.serviceTypeCode	)
      ps.setString( 7  ,	agentBean.pid	)
      ps.setString( 8  ,	agentBean.agentVersion	)
      ps.setString( 9  ,	agentBean.vmVersion	)
      ps.setString( 10  ,	agentBean.startTime	)
      ps.setString( 11  ,	agentBean.endTimestamp	)
      ps.setString( 12  ,	agentBean.endStatus	)
      ps.setString( 13  ,	agentBean.serverMetaData	)
      ps.setString( 14  ,	agentBean.jvmInfo	)
      ps.setString( 15  ,	agentBean.setServerMetaData	)
      ps.setString( 16  ,	agentBean.setJvmInfo	)
      ps.setString( 17  ,	agentBean.setHostname	)
      ps.setString( 18  ,	agentBean.setIp	)
      ps.setString( 19  ,	agentBean.setPorts	)
      ps.setString( 20  ,	agentBean.setAgentId	)
      ps.setString( 21  ,	agentBean.setApplicationName	)
      ps.setString( 22  ,	agentBean.setServiceType	)
      ps.setString( 23  ,	agentBean.setPid	)
      ps.setString( 24  ,	agentBean.setAgentVersion	)
      ps.setString( 25  ,	agentBean.setVmVersion	)
      ps.setString( 26  ,	agentBean.setStartTimestamp	)
      ps.setString( 27  ,	agentBean.setEndTimestamp	)
      ps.setString( 28  ,	agentBean.setEndStatus	)
      ps.setString( 29  ,	agentBean.container)

      println("************************存一个*************************")
      println()
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_agentTail(agentTailBean: AgentTailBean ) = {
    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null
    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into agent_tail values(?,?,?,?,?,?,NOW())")
      ps.setString( 1  ,	agentTailBean.version	)
      ps.setString( 2  ,	agentTailBean.agentId	)
      ps.setString( 3  ,	agentTailBean.startTimestamp	)
      ps.setString( 4  ,	agentTailBean.eventTimestamp	)
      ps.setString( 5  ,	agentTailBean.eventIdentifier	)
      ps.setString( 6  ,	agentTailBean.agentLifeCycleState	)

      println("************************存一个*************************")
      println()
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_str( strBean: StrBean ) = {

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into str values(?,?,?,?,?,?,?,?,NOW())")
      ps.setString(	1	, strBean.agentId	)
      ps.setString(	2	, strBean.startTime	)
      ps.setString(	3	, strBean.stringId	)
      ps.setString(	4	, strBean.stringValue	)
      ps.setString(	5	, strBean.setAgentId	)
      ps.setString(	6	, strBean.setAgentStartTime	)
      ps.setString(	7	, strBean.setStringId	)
      ps.setString(	8	, strBean.setStringValue	)

      println("************************存一个*************************")
      println()
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_api( apiBean: ApiBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into api values(?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())")
      ps.setString( 1 ,	apiBean.agentId	)
      ps.setString( 2 ,	apiBean.startTime	)
      ps.setString( 3 ,	apiBean.apiId	)
      ps.setString( 4 ,	apiBean.apiInfo	)
      ps.setString( 5 ,	apiBean.lineNumber	)
      ps.setString( 6 ,	apiBean.methodTypeEnum	)
      ps.setString( 7 ,apiBean.description)
      ps.setString( 8 ,	apiBean.setLine	)
      ps.setString( 9 ,	apiBean.setType	)
      ps.setString( 10 ,	apiBean.setAgentId	)
      ps.setString( 11 ,	apiBean.setAgentStartTime	)
      ps.setString( 12 ,	apiBean.setApiId	)
      ps.setString( 13 ,	apiBean.setApiInfo	)


      println("************************存一个*************************")
      println()
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_spanChuck(spanChuckBean: SpanChuckBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //
      ps=conn.prepareStatement("insert into spanChuck values(?,?,?,?,?,?,?,?,?,?,NOW())")
      ps.setString(1,	spanChuckBean.version	)
      ps.setString(2,	spanChuckBean.agentId	)
      ps.setString(3,	spanChuckBean.applicationId	)
      ps.setString(4,	spanChuckBean.agentStartTime	)
      ps.setString(5,	spanChuckBean.transactionId	)
      ps.setString(6,	spanChuckBean.spanId	)
      ps.setString(7,	spanChuckBean.endPoint	)
      ps.setString(8,	spanChuckBean.serviceType	)
      ps.setString(9,	spanChuckBean.applicationServiceType	)
      ps.setString(10,	spanChuckBean.spanEventBoList	)

      println("************************存一个*************************")
      println()
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_span(  spanBean: SpanBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into span values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())")
      ps.setString( 1  , spanBean.version)
      ps.setString( 2  , spanBean.agentId)
      ps.setString( 3  , spanBean.applicationId)
      ps.setString( 4  , spanBean.agentStartTime)
      ps.setString( 5  , spanBean.transactionId)
      ps.setString( 6  , spanBean.spanId)
      ps.setString( 7  , spanBean.parentSpanId)
      ps.setString( 8  , spanBean.parentApplicationId)
      ps.setString( 9  , spanBean.parentApplicationServiceType)
      ps.setString( 10  , spanBean.startTime)
      ps.setString( 11  , spanBean.elapsed)
      ps.setString( 12  , spanBean.rpc)
      ps.setString( 13  , spanBean.serviceType)
      ps.setString( 14  , spanBean.endPoint)
      ps.setString( 15  , spanBean.apiId)
      ps.setString( 16  , spanBean.annotationBoList)
      ps.setString( 17  , spanBean.flag)
      ps.setString( 18  , spanBean.errCode)
      ps.setString( 19  , spanBean.spanEventBoList)
      ps.setString( 20  , spanBean.collectorAcceptTime)
      ps.setString( 21  , spanBean.exceptionId)
      ps.setString( 22  , spanBean.exceptionMessage)
      ps.setString( 23  , spanBean.exceptionClass)
      ps.setString( 24  , spanBean.applicationServiceType)
      ps.setString( 25  , spanBean.acceptorHost)
      ps.setString( 26  , spanBean.remoteAddr)
      ps.setString( 27  , spanBean.loggingTransactionInfo)
      ps.setString( 28  , spanBean.root)
      ps.setString( 29  , spanBean.rawVersion)

      println("************************存一个*************************")
      println()
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_sql( sqlBean: SqlBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into `sql` values(?,?,?,?,?,?,?,?,?,NOW())")
      ps.setString( 1  ,	sqlBean.agentId	)
      ps.setString( 2  ,	sqlBean.startTime	)
      ps.setString( 3  ,	sqlBean.sqlId	)
      ps.setString( 4  ,	sqlBean.sql	)
      ps.setString( 5  ,	sqlBean.hashcode	)
      ps.setString( 6  ,	sqlBean.setAgentId	)
      ps.setString( 7  ,	sqlBean.setAgentStartTime	)
      ps.setString( 8  ,	sqlBean.setSqlId	)
      ps.setString( 9  ,	sqlBean.setSql	)

      println("************************存一个*************************")
      println()
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

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var ps1:PreparedStatement=null
    var ps2:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection

      var res:Boolean = true

      val	timeStamp = statBean.timestamp

      // 定义获取对应字段
      val field = statBean.field


      // 定义查询语句
      val SQLSentence = "select first_timestamp from stat order by first_timestamp ASC ; "
      ps = conn.prepareStatement(SQLSentence)
      rs = ps.executeQuery()

      while (rs.next()) {

        // 根据下标获取对应的
        val first_timestamp = rs.getString("first_timestamp")

        if (timeStamp.equals(first_timestamp)) {

          // 更新语句
          val updateSQL = "update stat set " + field   + "='" + statBean.statValues + "' where first_timestamp =  ? "
          ps1 = conn.prepareStatement( updateSQL )
          ps1.setString(1,timeStamp)
          ps1.executeUpdate()

          res = false

        }
      }
      if (res){
        // 插入语句
        val insertSQL = "insert into `stat` (" +  field  + ",first_timestamp,importDate) values (?,?,NOW())"

        ps2 = conn.prepareStatement(insertSQL)
        ps2.setString( 1  ,	statBean.statValues )
        ps2.setString( 2  ,	timeStamp	 )

        println("************************存一个*************************")
        println()
        ps2.executeUpdate()
      }

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



  def saveTo_unknown(  unknownBean: UnknownBean ): Unit = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into `unknown` values(?,NOW())")
      ps.setString(	1	, unknownBean.values	)

      println("************************存一个*************************")
      println()
      ps.executeUpdate()



    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
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