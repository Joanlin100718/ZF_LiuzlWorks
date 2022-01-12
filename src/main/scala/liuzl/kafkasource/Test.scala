package liuzl.kafkasource

import liuzl.dao.MysqlUtil

object Test {
  def main(args: Array[String]): Unit = {

    println(MysqlUtil.selectAndUpdateOffset("AIOPS_ETE_SERVFRAMETOPO" , 1 , 0))
  }

}
