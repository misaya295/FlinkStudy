package com.cwk.wc

import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object StreamWordCount {

  def main(args: Array[String]): Unit = {

    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val DS: DataStream[String] = environment.socketTextStream("hadoop103",7777);


    val value: DataStream[(String,Int)] = DS
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0).sum(1)

    value.print()
    environment.execute()

  }


}
