package com.alex.space.flink

import java.net.{InetAddress, InetSocketAddress}

import com.alex.space.connector.{ElasticSearchOutputFormat, ElasticsearchSinkFunction, RequestIndexer}
import com.alex.space.flink.config.Configable
import com.alex.space.flink.core.HBaseTableInputFormat
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType

import scala.collection.JavaConversions.{mapAsJavaMap, seqAsJavaList}
import scala.collection.mutable

/**
  * @author Alex
  *         Created by Alex on 2018/9/12.
  */
object HBaseToEsConnector extends Configable {

  override val prefix: String = "es"

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.createLocalEnvironment()
    val conf = new Configuration()
    conf.setString("akka.client.timeout", "10min")
    env.getConfig.setGlobalJobParameters(conf)

    env.setParallelism(1)

    val tableName = "test_table"

    val config = mutable.Map("bulk.flush.max.actions" -> "1000", "cluster.name" -> configString("cluster.name"))
    val hosts = configString("node.ips")

    val transports = hosts.split(",")
      .map(host => new InetSocketAddress(InetAddress.getByName(host), configInt("node.port"))).toList

    env.createInput(new HBaseTableInputFormat(tableName))
      .map(
        row => {
          row.getField(0).toString
        }
      )
      .output(new ElasticSearchOutputFormat[String](config, transports, new ElasticsearchSinkFunction[String] {
        def createIndexRequest(element: String): IndexRequest = {

          Requests.indexRequest.index("test_table2").`type`("d").id(element).source("{\"age\": \"20\"}", XContentType.JSON)
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
