package com.alex.space.flink

import java.util

import com.alex.space.flink.connector.ElasticsearchOutputFormat
import com.alex.space.flink.elasticsearch.ElasticSearchSinker
import com.alex.space.flink.hbase.{HBaseScanTableInputFormat, RowData}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}

/**
  * @author Alex
  *         Created by Alex on 2018/9/12.
  */
object SyncHBaseToEs {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    //      .createLocalEnvironment()
    val conf = new Configuration()
    conf.setString("akka.client.timeout", "10min")
    env.getConfig.setGlobalJobParameters(conf)

    env.setParallelism(2)

    val hbaseTable = "test_table"
    val hbaseCf = "d"

    val indexName = "test_table"
    val typeName = "d"

    val qualifiers: util.List[String] = _

    env.createInput(
      new HBaseScanTableInputFormat(hbaseTable, hbaseCf, qualifiers)
    )
      .map(row => buildJson(row))
      .output(
        new ElasticsearchOutputFormat[(String, XContentBuilder)](
          new ElasticSearchSinker(indexName, typeName))
      )

    println(env.getExecutionPlan)
    env.execute("Scan Hbase")

  }

  def buildJson(row: RowData): (String, XContentBuilder) = {

    val jsonBuilder = XContentFactory.jsonBuilder()
    // TODO build XContentBuilder

    (row.getRow, jsonBuilder)
  }
}
