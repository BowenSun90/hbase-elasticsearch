package com.alex.space.sync.core

import java.net.InetAddress

import com.alex.space.sync.config.Configable
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * @author Alex
  *         Created by Alex on 2018/9/11.
  */
object ElasticFactory extends Configable {

  override val prefix: String = "es"

  val address: String = configString("node.ips")
  val clusterName: String = configString("cluster.name")
  val clientPort: Int = configInt("node.port")

  var client: TransportClient = _

  def initClient() {

    if (client == null) {
      val endpoints = address.split(",").map(_.split(":", -1)).map {
        case Array(host, port) => new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)
        case Array(host) => new InetSocketTransportAddress(InetAddress.getByName(host), clientPort)
      }

      val settings = Settings.builder().put("cluster.name", clusterName).build()
      client = new PreBuiltTransportClient(settings)
        .addTransportAddresses(endpoints: _*)

    }
  }

  def getESClient: TransportClient = {
    try {
      initClient()
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        client = null
        initClient()
    }

    client
  }

}
