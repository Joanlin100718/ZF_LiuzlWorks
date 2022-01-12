package liuzl.dao


import com.mchange.v2.c3p0.ComboPooledDataSource
import liuzl.pojo.{AgentBean, AgentTailBean, ApiBean, SpanChuckBean, SpanBean, SqlBean, StatBean, StrBean, UnknownBean}

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

object MysqlUtil {
  
  val c3p0=new ComboPooledDataSource

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
  def saveTo_agentBean(agentBean: AgentBean ) = {
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
  def saveTo_agentTailBean(agentTailBean: AgentTailBean ) = {
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
  def saveTo_strBean( strBean: StrBean ) = {

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
  def saveTo_apiBean( apiBean: ApiBean ) = {

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
  def saveTo_spanChuckBean(spanChuckBean: SpanChuckBean ) = {

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
  def saveTo_spanBean(  spanBean: SpanBean ) = {

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
  def saveTo_sqlBean( sqlBean: SqlBean ) = {

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
  def saveTo_statBean( statBean: StatBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //SQL语句
      ps=conn.prepareStatement("insert into stat values(?,?,?,?,?,?,?,NOW())")
      ps.setString( 1  , 	statBean.agentId	)
      ps.setString( 2  , 	statBean.jvmGcBos	)
      ps.setString( 3  , 	statBean.jvmGcDetailedBos	)
      ps.setString( 4  , 	statBean.cpuLoadBos	)
      ps.setString( 5  , 	statBean.transactionBos	)
      ps.setString( 6  , 	statBean.activeTraceBos	)
      ps.setString( 7  , 	statBean.dataSourceListBos	)

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
  def saveTo_unknownBean(  unknownBean: UnknownBean ): Unit = {

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