package com.alex.space.flink.core

import java.net.InetAddress

import com.alex.space.flink.config.Configable
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.{XContentFactory, XContentType}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

/**
  * DataSet output elasticsearch
  *
  * @author Alex Created by Alex on 2018/9/12.
  */
class ElasticsearchOutputFormat extends OutputFormat[String] with Configable {

  val LOG: Logger = LoggerFactory.getLogger(classOf[ElasticsearchOutputFormat])

  override val prefix: String = "es"

  var client: TransportClient = _

  override def configure(configuration: Configuration): Unit = {
    System.setProperty("es.set.netty.runtime.available.processors", "false")

    val settings = Settings.builder().put("cluster.name", configString("cluster.name")).build()
    val nodes = configString("node.ips")
    val endpoints = nodes.split(",").map(_.split(":", -1))
      .map {
        case Array(host, port) => new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)
        case Array(host) => new InetSocketTransportAddress(InetAddress.getByName(host), configInt("node.port"))
      }
    client = new PreBuiltTransportClient(settings).addTransportAddresses(endpoints: _*)
  }

  override def writeRecord(element: String): Unit = {
    val jsonBuild = XContentFactory.jsonBuilder
    val builder = jsonBuild.startObject
    builder.field("name", "alex")
    builder.endObject()
    val json = builder.string()
    LOG.info("insert: " + json)

    try {
      val response = client.prepareIndex("test_table2", "d", element)
        .setSource(json, XContentType.JSON)
        .get()

      LOG.info("response: " + response)

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  override def close(): Unit = {
    client.close()
  }

  override def open(i: Int, i1: Int): Unit = {
    println("open")
  }
}
