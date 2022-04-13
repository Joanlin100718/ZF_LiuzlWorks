package liuzl.flink

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
/**
 * @author: Created By LiuZL
 * @company ChinaUnicom Software HeNan
 * @date: 2022-03-30 18:31
 * @version: v1.0
 * @description: liuzl.flink
 */
class MyKafkaDeserializationSchema  extends KafkaDeserializationSchema[ConsumerRecord[String, String]]{
    /*是否流结束，比如读到一个key为end的字符串结束，这里不再判断，直接返回false 不结束*/
    override def isEndOfStream(t: ConsumerRecord[String, String]): Boolean ={
        false
    }
    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): ConsumerRecord[String, String] = {

        new ConsumerRecord(record.topic(),record.partition(),record.offset(),new String(record.key(),"UTF-8"),new String(record.value(),"UTF-8"))

    }
    /*用于获取反序列化对象的类型*/
    override def getProducedType: TypeInformation[ConsumerRecord[String, String]] = {
        TypeInformation.of(new TypeHint[ConsumerRecord[String, String]] {})
    }
}
