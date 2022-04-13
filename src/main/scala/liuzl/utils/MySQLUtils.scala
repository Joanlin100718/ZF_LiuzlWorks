package liuzl.utils

import com.alibaba.druid.pool.DruidDataSourceFactory
import liuzl.dao.MysqlUtil_SysOamp_Batch.getDateFromTimeStamp
import liuzl.pojo.StatBean

import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData}
import java.util
import java.util.Collections.list
import java.util.ResourceBundle
import javax.sql.DataSource
import scala.collection.mutable.ListBuffer

object MySQLUtils {


    // 获取配置文件
    val config : ResourceBundle = GetPropertiesUtils.getConfig("jdbc")  //不需要写后缀名

    // 创建连接池对象
    var dataSource:DataSource = init()

    // 连接池的初始化
    def init():DataSource = {

        val paramMap = new java.util.HashMap[String, String]()
        paramMap.put("driverClassName", config.getObject("driver").toString)
        paramMap.put("url", config.getObject("jdbcUrl").toString)
        paramMap.put("username", config.getObject("user").toString)
        paramMap.put("password", config.getObject("password").toString)
        paramMap.put("maxActive", config.getObject("poolSize").toString)

        // 使用Druid连接池对象
        DruidDataSourceFactory.createDataSource(paramMap)
    }

    // 从连接池中获取连接对象
    def getConnection(): Connection = {
        dataSource.getConnection
    }


    // 从连接池中获取连接对象
    def selectAll(tbName : String): ResultSet = {
        val conn : Connection = getConnection()
        var ps : PreparedStatement = null
        var rs : ResultSet = null
        var metaData : ResultSetMetaData = null
        var columnCount : Int = 0

        val selectSql = "select * from " + tbName + ";"

        ps = conn.prepareStatement(selectSql)

        rs = ps.executeQuery()

        metaData  = rs.getMetaData()

        columnCount = metaData.getColumnCount();

        while (rs.next()){
            for (i <- 1.to(columnCount)) {
//                print(metaData.getColumnType(i)+ "\t")
                print(rs.getString(metaData.getColumnName(i)) + "\t")
            }
            println()
        }
        rs
    }


    // 从连接池中获取连接对象
    def selectData(selectSql : String ): ResultSet = {
        val conn : Connection = getConnection()
        var ps : PreparedStatement = null
        var rs : ResultSet = null

        try{
            ps = conn.prepareStatement(selectSql)
            rs = ps.executeQuery()
        } catch {
            case t: Throwable => t.printStackTrace() // TODO: handle error
        } finally {
            if (conn != null){
                conn.close()
            }
            if (ps != null){
                ps.close()
            }
            if (rs != null){
                rs.close()
            }
        }

        rs
    }



    /*
    *   插入更新方法
    *   需要对应表拥有“唯一主键”
    *
    * */
    def insertDataOrUpdate( list: List[Int]  ) = {

        val conn:Connection = getConnection()
        var ps:PreparedStatement = null

        try {

            val insertUpdateSql = "INSERT INTO test (col1, col2 , col3 , col4) values (?,?,?,?) ON DUPLICATE KEY UPDATE col2= VALUES(col2) , col3= VALUES(col3) , col4= VALUES(col4) ;"

            ps = conn.prepareStatement(insertUpdateSql)
            ps.setString(1,list(0).toString)
            ps.setString(2,list(1).toString)
            ps.setString(3,list(2).toString)
            ps.setString(4,list(3).toString)

            println(ps)


            ps.executeUpdate()

        } catch {
            case t: Throwable => t.printStackTrace() // TODO: handle error
        }finally {
            if (conn != null){
                conn.close()
            }
            if (ps != null){
                ps.close()
            }
        }
    }


    def main(args: Array[String]): Unit = {

        for ( i <- 0 to 100) {
            val list = RandomNumUtils.randomMultiInt(1,50,4,true);
            insertDataOrUpdate(list)
        }


//        val t3=(1,2,(Array(3,4),5))
//        println(t3.getClass)
//        println(t3)


//        selectAll("app_record_operations")
    }



/*    // 从连接池中获取连接对象
    def selectData(selectSql : String ): ResultSet = {
        val conn : Connection = getConnection()
        var ps : PreparedStatement = null
        var rs : ResultSet = null
        var metaData : ResultSetMetaData = null
        var columnCount : Int = 0
        ps = conn.prepareStatement(selectSql)
        rs = ps.executeQuery()
        metaData  = rs.getMetaData()
        columnCount = metaData.getColumnCount();
        while (rs.next()){
            for (i <- 1.to(columnCount)) {
                //                print(metaData.getColumnType(i)+ "\t")
                print(rs.getString(metaData.getColumnName(i)) + "\t")
            }
            println()
        }
        rs
    }*/



}
