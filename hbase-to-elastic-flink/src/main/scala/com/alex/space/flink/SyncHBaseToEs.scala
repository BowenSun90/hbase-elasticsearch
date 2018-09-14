package com.alex.space.flink

import java.text.SimpleDateFormat

import com.alex.space.common.{CommonConstants, DataTypeEnum}
import com.alex.space.flink.elasticsearch.{ElasticSearchOutputFormat, ElasticSearchSinker}
import com.alex.space.flink.hbase.HBaseTableInputFormat
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.elasticsearch.common.xcontent.XContentFactory

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

    val hbaseTable = "test_table"
    val indexName = "test_table2"
    val typeName = "d"

    env.createInput(new HBaseTableInputFormat(hbaseTable))
      .map(
        row => {
          (row.getField(0).toString, buildJson(row.f1))
        }
      )
      .output(new ElasticSearchOutputFormat(new ElasticSearchSinker(indexName, typeName)))

    println(env.getExecutionPlan)
    val result = env.execute("Scan Hbase").getAllAccumulatorResults
    println(result)

  }

  def buildJson(cells: Array[Cell]): String = {
    val jsonBuild = XContentFactory.jsonBuilder
    val builder = jsonBuild.startObject
    for (cell: Cell <- cells) {
      try {

        val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))

        if (qualifier.startsWith(DataTypeEnum.StringArray.getKeyName)) {

          val arrNode = new ObjectMapper().readTree(value)
          val arr: Array[String] = new Array[String](arrNode.size())
          for (i <- 0 until arrNode.size()) {
            arr(i) = arrNode.get(i).asText()
          }
          builder.array(qualifier, arr)

        } else if (qualifier.startsWith(DataTypeEnum.BoolArray.getKeyName)) {

          val arrNode = new ObjectMapper().readTree(value)
          val arr: Array[Boolean] = new Array[Boolean](arrNode.size())

          for (i <- 0 until arrNode.size()) {
            arr(i) = arrNode.get(i).asBoolean()
          }
          builder.array(qualifier, arr)

        } else if (qualifier.startsWith(DataTypeEnum.NumberArray.getKeyName)) {

          val arrNode = new ObjectMapper().readTree(value)
          val arr: Array[Double] = new Array[Double](arrNode.size())
          for (i <- 0 until arrNode.size()) {
            arr(i) = arrNode.get(i).asDouble()
          }
          builder.array(qualifier, arr)

        } else if (qualifier.startsWith(DataTypeEnum.String.getKeyName)) {

          builder.field(qualifier, value)

        } else if (qualifier.startsWith(DataTypeEnum.Number.getKeyName)) {

          builder.field(qualifier, value.toDouble)

        } else if (qualifier.startsWith(DataTypeEnum.Bool.getKeyName)) {

          builder.field(qualifier, value.toBoolean)

        } else if (qualifier.startsWith(DataTypeEnum.Date.getKeyName)) {

          val sdf = new SimpleDateFormat(CommonConstants.DATE_FORMAT)
          builder.field(qualifier, sdf.parse(value))

        } else if (qualifier.startsWith(DataTypeEnum.Json.getKeyName)) {

          builder.field(qualifier, value)

        }
      } catch {
        case e: Exception => e.printStackTrace()
      }

    }
    builder.endObject()
    builder.string()
  }

}
