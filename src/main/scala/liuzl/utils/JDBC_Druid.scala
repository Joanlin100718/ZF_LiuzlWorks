package liuzl.utils

import com.alibaba.druid.Constants

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Statement}
import java.util.Properties
import com.alibaba.druid.pool.DruidDataSourceFactory

import javax.sql.DataSource
import org.slf4j.LoggerFactory

/**
 * Druid 连接池工具类
 *
 * create by LiuJinHe 2020/8/5
 */
object JDBC_Druid {

  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  // 获取Druid连接池的配置文件
  private  val pro: Properties = new Properties
  // 初始化线程池
  private var ds: DataSource = null

  pro.load(JDBC_Druid.getClass.getClassLoader.getResourceAsStream("druid.properties"))


  def initDataSource(): DataSource = {
    if(ds == null){
      try {
        ds = DruidDataSourceFactory.createDataSource(pro)
      } catch {
        case e: Exception =>
          logger.error("初始化连接池失败...", e)
      }
    }
    ds
  }


  /**
   * 获取连接
   */
  def getConnection: Connection = {

    initDataSource().getConnection()

  }


  /**
   * 提交事务
   */
  def commit(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.commit()
      } catch {
        case e: SQLException =>
          logger.error(s"提交数据失败,conn: $conn", e)
      } finally {
        close(conn)
      }
    }
  }

  /**
   * 事务回滚
   */
  def rollback(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.rollback()
      } catch {
        case e: SQLException =>
          logger.error(s"事务回滚失败,conn: $conn", e)
      } finally {
        close(conn)
      }
    }
  }

  /**
   * 关闭连接
   */
  def close(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e: SQLException =>
          logger.error(s"关闭连接失败,conn: $conn", e)
      }
    }
  }

  /**
   * 关闭连接
   */
  def close(ps: PreparedStatement, conn: Connection): Unit = {
    if (ps != null) {
      try {
        ps.close()
      } catch {
        case e: SQLException =>
          logger.error(s"关闭连接失败,ps: $ps", e)
      }
    }
    close(conn)
  }

  /**
   * 关闭连接
   */
  def close(ps: PreparedStatement, conn: Connection , rs :ResultSet): Unit = {
    if (ps != null) {
      try {
        ps.close()
      } catch {
        case e: SQLException =>
          logger.error(s"关闭连接失败,ps: $ps", e)
      }
    }
    if (rs != null) {
      try {
        rs.close()
      } catch {
        case e: SQLException =>
          logger.error(s"关闭连接失败,rs: $rs", e)
      }
    }
    close(conn)
  }

  /**
   * 关闭连接
   */
  def close(stat: Statement, conn: Connection): Unit = {
    if (stat != null) {
      try {
        stat.close()
      } catch {
        case e: SQLException =>
          logger.error(s"关闭连接失败,stat: $stat", e)
      }
    }
    close(conn)
  }



}
