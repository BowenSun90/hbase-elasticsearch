package com.alex.space

import java.net.{InetAddress, InetSocketAddress}

import com.alex.space.connector.{ElasticSearchOutputFormat, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType

import scala.collection.JavaConversions.{mapAsJavaMap, seqAsJavaList}
import scala.collection.mutable

/**
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]) {
    val config = mutable.Map("bulk.flush.max.actions" -> "1000", "cluster.name" -> "elasticsearch")
    val hosts = "elasticsearch01.com"

    val transports = hosts.split(",").map(host => new InetSocketAddress(InetAddress.getByName(host), 9300)).toList

    val env = ExecutionEnvironment.createLocalEnvironment()

    val data: DataSet[String] = env.fromElements("1", "2")

    data.output(new ElasticSearchOutputFormat[String](config, transports, new ElasticsearchSinkFunction[String] {

      def createIndexRequest(element: String): IndexRequest = {

        Requests.indexRequest.index("test_table2").`type`("d").id(element).source("{\"age\": \"30\"}", XContentType.JSON)
      }

      override def process(element: String, ctx: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        requestIndexer.add(createIndexRequest(element))
      }

    }))

    println(env.getExecutionPlan())
    env.execute("test")
  }

}
