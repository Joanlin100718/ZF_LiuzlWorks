package liuzl.dao

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import liuzl.pojo.AgentBean

import scala.collection.immutable
import scala.io.Source

object KafkaTest {
  def main(args: Array[String]): Unit = {

    val lines: Iterator[String] = Source.fromFile("D:\\data01.txt").getLines()
    val list: List[String] = lines.toList
    val jSONObjects: immutable.Seq[JSONObject] = list.map(x => {  //取出每一条数据，把数据转换成JSONObject类型
      println(x)
      JSON.parseObject(x)
    })
    jSONObjects.foreach(resJson=>{
      val		hostname		= resJson.getString("hostname")
      val		ip		= resJson.getString("ip")
      val		ports		= resJson.getString("ports")
      val		agentId		= resJson.getString("agentId")
      val		applicationName		= resJson.getString("applicationName")
      val		serviceType		= resJson.getString("serviceType")
      val		pid		= resJson.getString("pid")
      val		agentVersion		= resJson.getString("agentVersion")
      val		vmVersion		= resJson.getString("vmVersion")
      val		startTimestamp		= resJson.getString("startTimestamp")
      val		endTimestamp		= resJson.getString("endTimestamp")
      val		endStatus		= resJson.getString("endStatus")
      val		serverMetaData		= resJson.getString("serverMetaData")
      val		jvmInfo		= resJson.getString("jvmInfo")
      val		setServerMetaData		= resJson.getString("setServerMetaData")
      val		setJvmInfo		= resJson.getString("setJvmInfo")
      val		setHostname		= resJson.getString("setHostname")
      val		setIp		= resJson.getString("setIp")
      val		setPorts		= resJson.getString("setPorts")
      val		setAgentId		= resJson.getString("setAgentId")
      val		setApplicationName		= resJson.getString("setApplicationName")
      val		setServiceType		= resJson.getString("setServiceType")
      val		setPid		= resJson.getString("setPid")
      val		setAgentVersion		= resJson.getString("setAgentVersion")
      val		setVmVersion		= resJson.getString("setVmVersion")
      val		setStartTimestamp		= resJson.getString("setStartTimestamp")
      val		setEndTimestamp		= resJson.getString("setEndTimestamp")
      val		setEndStatus		= resJson.getString("setEndStatus")

//      val agentBean = AgentBean(hostname,ip,ports,agentId,applicationName,serviceType,pid,agentVersion,vmVersion,startTimestamp,endTimestamp,endStatus,serverMetaData,jvmInfo,setServerMetaData,setJvmInfo,setHostname,setIp,setPorts,setAgentId,setApplicationName,setServiceType,setPid,setAgentVersion,setVmVersion,setStartTimestamp,setEndTimestamp,setEndStatus)


//      println("agentBean:" + agentBean.toString)

      // 将数据存储到MySQL
//      MysqlUtil_Test.saveToMysql(agentBean)


    })


  }

}
