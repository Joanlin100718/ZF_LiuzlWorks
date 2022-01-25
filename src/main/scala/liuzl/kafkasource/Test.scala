package liuzl.kafkasource

import kafka.common.TopicAndPartition
import liuzl.dao.MysqlUtil

object Test {
  def main(args: Array[String]): Unit = {


    val list = scala.collection.mutable.ListBuffer(1,2,3,4,5,6,7,8,9)


    for (i <- list){
      println(i)
      println(list(0))
    }

    var str ="qweqweqweq"
    println(str.concat("12313213"))

    val a = null
    println(Option(a).toString)


//    val str = "({\"agentId\":\"192.168.165.199-9999\",\"startTimestamp\":1642059748664,\"timestamp\":1642060478824,\"directCount\":170,\"directMemoryUsed\":2977697,\"mappedCount\":0,\"mappedMemoryUsed\":0,\"agentStatType\":\"DIRECT_BUFFER\"},{\"agentId\":\"192.168.165.199-9999\",\"startTimestamp\":1642059748664,\"timestamp\":1642060483824,\"directCount\":170,\"directMemoryUsed\":2977697,\"mappedCount\":0,\"mappedMemoryUsed\":0,\"agentStatType\":\"DIRECT_BUFFER\"},{\"agentId\":\"192.168.165.199-9999\",\"startTimestamp\":1642059748664,\"timestamp\":1642060488824,\"directCount\":170,\"directMemoryUsed\":2977697,\"mappedCount\":0,\"mappedMemoryUsed\":0,\"agentStatType\":\"DIRECT_BUFFER\"},{\"agentId\":\"192.168.165.199-9999\",\"startTimestamp\":1642059748664,\"timestamp\":1642060493824,\"directCount\":170,\"directMemoryUsed\":2977697,\"mappedCount\":0,\"mappedMemoryUsed\":0,\"agentStatType\":\"DIRECT_BUFFER\"},{\"agentId\":\"192.168.165.199-9999\",\"startTimestamp\":1642059748664,\"timestamp\":1642060498824,\"directCount\":170,\"directMemoryUsed\":2977697,\"mappedCount\":0,\"mappedMemoryUsed\":0,\"agentStatType\":\"DIRECT_BUFFER\"},{\"agentId\":\"192.168.165.199-9999\",\"startTimestamp\":1642059748664,\"timestamp\":1642060503824,\"directCount\":170,\"directMemoryUsed\":2977697,\"mappedCount\":0,\"mappedMemoryUsed\":0,\"agentStatType\":\"DIRECT_BUFFER\"})"
//
//
//    val i = str.indexOf("timestamp")
//    // 77
//    println(str.take(i + 24).takeRight(13))
//    println(str.indexOf("timestamp"))



//    MysqlUtil.saveTo_stat_test()


//    val list = scala.collection.mutable.ListBuffer("1")
//    list.remove(0)
//
//    list.append("partition0")
//    list.append("partition1")
//    list.append("partition2")
//    list.append("partition3")
//    list.append("partition4")
//    list.append("partition5")
//    println(list)
//
//    println(MysqlUtil.selectOffsetList("AIOPS_ETE_SERVFRAMETOPO"))

//    val topics =
//      "AIOPS_ETE_SERVFRAMETOPO,"  +  //span
//        "AIOPS_ETE_SERVCHUNKTOPO,"  +   //spanChunk
//        "AIOPS_ETE_SERVSTATTOPO,"   +   //stat
//        "AIOPS_ETE_SERVAGENTTOPO," +    //agent
//        "AIOPS_ETE_SERVAPITOPO," +     //api
//        "AIOPS_ETE_SERVSTRTOPO,"  +     //str
//        "AIOPS_ETE_SERVUNKNOWN,"   +     // unknown
//        "AIOPS_ETE_SERVSQLTOPO"       //sql
//    val topicLists = topics.split(",").toList
//    var index = 0
//    for (topics <- topicLists){
//      println(index)
//      index += 1
//      println(topics)
//    }


//    // 定义返回值类型
//    var fromOffsets: Map[TopicAndPartition, Long] = Map()
//
//    // 将topics列表转换为List
//    val topicLists = topics.split(",").toList
//
//    // 遍历List 得到数据
//    for (topic <- topicLists){
//
//      // 获取各个partition对应的offset
//      val lists = MysqlUtil.selectOffsetList(topic)
//
//      // 定义数据下标
//      var index = 0
//
//      // 遍历offset列表
//      for (list <- lists){
//
//        // 定义返回Map值
//        val tp = TopicAndPartition(topic,index)
//        val offset = list
//        index += 1
//        println(tp)
//        fromOffsets+= (tp -> offset)
//      }
//    }
//    println(fromOffsets)

    //    println(MysqlUtil.selectAndUpdateOffset("AIOPS_ETE_SERVFRAMETOPO" , 1 , 0))
  }

}
