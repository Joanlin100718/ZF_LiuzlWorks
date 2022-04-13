package liuzl.dao

import com.mchange.v2.c3p0.ComboPooledDataSource
import liuzl.pojo._

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.collection.mutable.ListBuffer

object MysqlUtil_User {
  
  val c3p0 = new ComboPooledDataSource("UserSource")

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
      val SQLSentence = "select partition0 from historicalOffset where kafkaTopic = \"" + topic  + "\" ;"
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
  def updateKafkaOffset(topic:String, partition : Int, offset:Long) : Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null

    try {
      conn = c3p0.getConnection

      // 更新offset 语句
      val updateSQL = "update historicalOffset set partition" + partition + "  = ?  where kafkaTopic =  ? "

      ps = conn.prepareStatement(updateSQL)
      ps.setLong(1, offset)
      ps.setString(2, topic)

//      println("****************************offset已更新*********************************")

      ps.executeUpdate()


    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    } finally {
      if (ps != null) ps.close
      if (rs != null) rs.close
      if (conn != null) conn.close
    }

  }

  /*
  * 更新用户
  *
  * */
  def updateTo_Jq_Register(  userBean: UserBean ): Unit = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , ChunkBean: ChunkBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection

      // 定义列表，用于存储 字段名、对应值。
      val lists  = scala.collection.mutable.ListBuffer("")
      lists.remove(0)

      if (! Option(userBean.userId).toString.equals("None")){
        lists.append("userId")
        lists.append(userBean.userId)
      }
      if (! Option(userBean.mobile).toString.equals("None")){
        lists.append("mobile")
        lists.append(userBean.mobile)
      }
      if (! Option(userBean.userName).toString.equals("None")) {
        lists.append("userName")
        lists.append(userBean.userName)
      }
      if (! Option(userBean.password).toString.equals("None")){
        lists.append("password")
        lists.append(userBean.password)
      }
      if (! Option(userBean.nickName).toString.equals("None")){
        lists.append("nickName")
        lists.append(userBean.nickName)
      }
      if (! Option(userBean.phonenumber).toString.equals("None") ){
        lists.append("phonenumber")
        lists.append(userBean.phonenumber)
      }
      if (! Option(userBean.userType).toString.equals("None")){
        lists.append("userType")
        lists.append(userBean.userType)
      }
      if (! Option(userBean.params).toString.equals("None") ){
        lists.append("params")
        lists.append(userBean.params)
      }

      // SQL 前置语句
      var splicingSqlSentence :String = "update `jq_register` set "

      // 对更新语句进行拼接
      for (pointer <- 0 to lists.length-1 by 2 ){
        splicingSqlSentence = splicingSqlSentence.concat( " " + lists(pointer) + "='" + lists(pointer + 1 ) + "'," )
      }

      // 定义更新数据库语句
      var  updateSqlSentence = ""

      // 判断特殊字段 admin 是否作为更新字段 并且进行更新语句的拼接
      if (! Option(userBean.admin).toString.equals("None") ){
        updateSqlSentence = splicingSqlSentence + "admin = " + userBean.admin + " where userId = '" + userBean.userId + "';"
      } else {
        updateSqlSentence = splicingSqlSentence.dropRight(1) +  " where userId = '" + userBean.userId + "';"
      }

      // 提交更新sql语句
      ps = conn.prepareStatement(updateSqlSentence)
//      println("****************************更新了一个*********************************")
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
  * 添加用户
  *
  * */
  def saveTo_Jq_Register(  userBean: UserBean ): Unit = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , ChunkBean: ChunkBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //SQL语句
      val sqlSentence = "insert into `jq_register`(userId,mobile,userName,password,nickName,phonenumber,userType,admin,params,updateTime) values(?,?,?,?,?,?,?,?,?,NOW())"
      ps=conn.prepareStatement(sqlSentence)
      ps.setString(	1	, userBean.userId	)
      ps.setString(	2	, userBean.mobile	)
      ps.setString(	3	, userBean.userName	)
      ps.setString(	4	, userBean.password	)
      ps.setString(	5	, userBean.nickName	)
      ps.setString(	6	, userBean.phonenumber	)
      ps.setString(	7	, userBean.userType	)
      ps.setBoolean(8	, userBean.admin)
      ps.setString(	9	, userBean.params	)

//      println("************************存一个*************************")
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
  * 删除用户
  *
  * */

  def deleteTo_Jq_Register(  delUserId: String ): Unit = {

    //     agentBean: AgentBean * ,apiBean: ApiBean , ChunkBean: ChunkBean,spanBean: SpanBean,sqlBean: SqlBean,statBean: StatBean,strBean: StrBean

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    try {
      conn=c3p0.getConnection
      //SQL语句
      val sqlSentence = "delete from `jq_register` where userId = ? "
      ps=conn.prepareStatement(sqlSentence)
      ps.setString(	1	, delUserId	)

//      println("************************删了一个*************************")
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