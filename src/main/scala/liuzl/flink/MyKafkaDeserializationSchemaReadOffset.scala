package liuzl.flink

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import liuzl.flink.MyKafkaDeserializationSchema



class MyKafkaDeserializationSchemaReadOffset extends KafkaDeserializationSchema[String] {

    // 该方法的使用方法
//    val consumer = new FlinkKafkaConsumer[](topic, new MyKafkaDeserializationSchema, KafkaProperties)
//    val UTF_8 : cs = cs.forName("UTF-8")

    override def isEndOfStream(t: String): Boolean = {
        false
    }

    override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]) : String = {
        val value = new String(consumerRecord.value(), "GBK")
        val offset = consumerRecord.offset()
        val partition = consumerRecord.partition()

//        println("partition:" + partition + "\toffset:" + offset)
        // Scala中将Object转JsonObject 不像java一样支持强转
        val str = JSON.toJSON(value).toString
        val  jsonObject : JSONObject = JSON.parseObject(str)

        /*
        // method:1
        val str1 = JSON.toJSON(value).toString
        val eventJson1 = JSON.parseObject(str)
        // method:2
        val str2 = JSON.toJSONString(value,SerializerFeature.WriteMapNullValue)
        val eventJson2 = JSON.parseObject(str)
        // method:3
        val eventJson3 = JSON.toJSON(value).asInstanceOf[JSONObject]
        */

        jsonObject.put("offset",offset)
        jsonObject.put("partition",partition)
        jsonObject.toJSONString
        val a = value + partition  + offset
        a
//        offset + "" + partition
    }

    override def getProducedType : TypeInformation[String] = {
        null
    }
}
