package liuzl.flink



object Test {
    def main(args: Array[String]): Unit = {
        val list = List("hello world","Hello Scala","Hello Joan")
        val r1 = list.flatMap {
            x => x.split(" ")
        }.map{x => (x,1)}.groupBy(x => x._1)

        val r2 = r1.mapValues{list => list.map(x => x._2).sum}

        println(r2)





    }

}
