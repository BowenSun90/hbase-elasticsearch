package com.alex.space.flink

import com.alex.space.flink.elasticsearch.{ElasticSearchOutputFormat, ElasticSearchSinker}
import com.alex.space.flink.hbase.HBaseTableInputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

/**
  * @author Alex
  *         Created by Alex on 2018/9/12.
  */
object SyncHBaseToEs {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.createLocalEnvironment()
    val conf = new Configuration()
    conf.setString("akka.client.timeout", "10min")
    env.getConfig.setGlobalJobParameters(conf)

    env.setParallelism(1)

    val tableName = "test_table"

    env.createInput(new HBaseTableInputFormat(tableName))
      .map(
        row => {
          row.getField(0).toString
        }
      )
      .output(new ElasticSearchOutputFormat(new ElasticSearchSinker))

    println(env.getExecutionPlan)
    val result = env.execute("Scan Hbase").getAllAccumulatorResults
    println(result)

  }

}
