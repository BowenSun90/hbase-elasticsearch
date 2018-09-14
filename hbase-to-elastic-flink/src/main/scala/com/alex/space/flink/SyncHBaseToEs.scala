package com.alex.space.flink

import com.alex.space.flink.core.HBaseTableInputFormat
import com.alex.space.flink.elasticsearch.{ElasticSearchOutputFormat, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType

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
      .output(new ElasticSearchOutputFormat(new ElasticsearchSinkFunction[String] {
        def createIndexRequest(element: String): IndexRequest = {

          Requests.indexRequest.index("test_table2").`type`("d").id(element).source("{\"age\": \"30\"}", XContentType.JSON)
        }

        override def process(element: String, ctx: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(element))
        }
      }
      ))

    println(env.getExecutionPlan)
    val result = env.execute("Scan Hbase").getAllAccumulatorResults
    println(result)

  }

}
