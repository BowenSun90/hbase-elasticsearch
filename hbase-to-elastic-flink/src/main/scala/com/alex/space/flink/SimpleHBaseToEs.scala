package com.alex.space.flink

import com.alex.space.flink.core.{ElasticsearchOutputFormat, HBaseTableInputFormat}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * @author Alex
  *         Created by Alex on 2018/9/12.
  */
object SimpleHBaseToEs {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.createLocalEnvironment()
    env.setParallelism(1)

    val hbaseTable = "test_table"

    env.createInput(new HBaseTableInputFormat(hbaseTable))
      .map(
        row => {
          row.getField(0).toString
        }
      )
      .output(new ElasticsearchOutputFormat())

    println(env.getExecutionPlan)
    val result = env.execute("Scan Hbase").getAllAccumulatorResults
    println(result)

  }

}
