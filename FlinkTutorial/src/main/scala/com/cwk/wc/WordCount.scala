package com.cwk.wc

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


    val inputDateSet: DataSet[String] = env.readTextFile("/Users/chenwenkang/IdeaProjects/FlinkStudy/FlinkTutorial/src/main/resources/wc.txt")


    val value: DataSet[(String,Int)] = inputDateSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0).sum(1)

    value.print()







  }

}
