package liuzl.flink


import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.AggregateDataSet
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.common.serialization.Serdes
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.mutable.Map

object WordCountByFile {
    def main(args: Array[String]): Unit = {

        val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val source = env.readTextFile("D:\\CodeStation\\Datas\\test01.txt")



        val counts  = source.flatMap{_.toLowerCase.split(" ").filter{_.nonEmpty}}
          .map{(_,1)}
          .keyBy(0)
          .sum(1)


        counts.print()

        val a = counts.name
        println(a)


        env.execute()
    }
}
