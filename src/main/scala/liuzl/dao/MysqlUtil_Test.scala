package liuzl.dao

import com.mchange.v2.c3p0.ComboPooledDataSource
import liuzl.pojo.AgentBean

import java.sql.{Connection, PreparedStatement, ResultSet}

object MysqlUtil_Test {
  
  val c3p0=new ComboPooledDataSource
  
  /**
   * 将实时统计出的业务指标数据，插入到mysql  weblog库下的tongji2表
   */
  def saveToMysql(agentBean: AgentBean) = {
    
    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null
    
    try {
      conn=c3p0.getConnection
      //如果当天还没有数据,则新增插入
      ps=conn.prepareStatement("insert into test01 values(?,?)")
      ps.setString(1, agentBean.hostName)
      ps.setString(2, agentBean.ip)
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