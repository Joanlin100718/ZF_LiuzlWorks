package liuzl.dao


import com.mchange.v2.c3p0.ComboPooledDataSource
import liuzl.pojo.{AgentBean, AgentTailBean, ApiBean, ChuckBean, SpanBean, SqlBean, StatBean, StrBean, UnknownBean}

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

object MysqlUtil {
  
  val c3p0=new ComboPooledDataSource
  
  /**
   * 数据存储到对应表中
   */
  def saveTo_agentBean(agentBean: AgentBean ) = {
    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null
    try {
      conn=c3p0.getConnection
      //如果当天还没有数据,则新增插入
      ps=conn.prepareStatement("insert into agent values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
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
      //如果当天还没有数据,则新增插入
      ps=conn.prepareStatement("insert into agent_tail values(?,?,?,?,?,?)")
      ps.setString( 1  ,	agentTailBean.version	)
      ps.setString( 2  ,	agentTailBean.agentId	)
      ps.setString( 3  ,	agentTailBean.startTimestamp	)
      ps.setString( 4  ,	agentTailBean.eventTimestamp	)
      ps.setString( 5  ,	agentTailBean.eventIdentifier	)
      ps.setString( 6  ,	agentTailBean.agentLifeCycleState	)

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
      //如果当天还没有数据,则新增插入
      ps=conn.prepareStatement("insert into str values(?,?,?,?,?,?,?,?)")
      ps.setString(	1	, strBean.agentId	)
      ps.setString(	2	, strBean.startTime	)
      ps.setString(	3	, strBean.stringId	)
      ps.setString(	4	, strBean.stringValue	)
      ps.setString(	5	, strBean.setAgentId	)
      ps.setString(	6	, strBean.setAgentStartTime	)
      ps.setString(	7	, strBean.setStringId	)
      ps.setString(	8	, strBean.setStringValue	)
      println("开始存储喽")
      println(ps)
      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_apiBean( topic:String ,  apiBean: ApiBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //如果当天还没有数据,则新增插入
      topic match{
        case str => {
          ps=conn.prepareStatement("insert into str values(?,?,?,?,?,?,?,?)")
          ps.setString(	1	, ""	)
          ps.executeUpdate()
        }
      }


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_chuckBean( topic:String ,  chuckBean: ChuckBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //如果当天还没有数据,则新增插入
      topic match{
        case str => {
          ps=conn.prepareStatement("insert into str values(?,?,?,?,?,?,?,?)")
          ps.setString(	1	, ""	)
          ps.executeUpdate()
        }
      }


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_spanBean( topic:String ,  spanBean: SpanBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //如果当天还没有数据,则新增插入
      topic match{
        case str => {
          ps=conn.prepareStatement("insert into str values(?,?,?,?,?,?,?,?)")
          ps.setString(	1	, ""	)
          ps.executeUpdate()
        }
      }


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_sqlBean( topic:String ,  sqlBean: SqlBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //如果当天还没有数据,则新增插入
      topic match{
        case str => {
          ps=conn.prepareStatement("insert into str values(?,?,?,?,?,?,?,?)")
          ps.setString(	1	, ""	)
          ps.executeUpdate()
        }
      }


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_statBean( topic:String ,  statBean: StatBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //如果当天还没有数据,则新增插入
      topic match{
        case str => {
          ps=conn.prepareStatement("insert into str values(?,?,?,?,?,?,?,?)")
          ps.setString(	1	, ""	)
          ps.executeUpdate()
        }
      }


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }
  def saveTo_unknownBean( topic:String ,  unknownBean: UnknownBean ) = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , chuckBean: ChuckBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //如果当天还没有数据,则新增插入
      topic match{
        case str => {
          ps=conn.prepareStatement("insert into str values(?,?,?,?,?,?,?,?)")
          ps.setString(	1	, ""	)
          ps.executeUpdate()
        }
      }


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if(ps!=null)ps.close
      if(rs!=null)rs.close
      if(conn!=null)conn.close
    }
  }

}