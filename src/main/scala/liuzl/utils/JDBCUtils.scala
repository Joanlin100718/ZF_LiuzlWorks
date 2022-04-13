package liuzl.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import com.alibaba.druid.pool.DruidDataSourceFactory

import java.util.ResourceBundle
import javax.sql.DataSource

object JDBCUtils {

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

    def main(args: Array[String]): Unit = {

        println(getConnection())

        val conn : Connection  = getConnection();
        var ps: PreparedStatement = null;
        var rs : ResultSet = null;


        ps = conn.prepareStatement("select * from app_record_operations")

        rs = ps.executeQuery()
 
        while(rs.next()){
            print(rs.getString(1) + "\t")
            print(rs.getString(2) + "\t")
            print(rs.getString(3) + "\t")
            print(rs.getString(4) + "\t")
            print(rs.getString(5) + "\t")
            println()
        }


    }
}
