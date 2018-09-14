package com.alex.space.sync

import com.alex.space.sync.core.ElasticFactory
import org.apache.spark.SparkContext
import org.elasticsearch.index.query.QueryBuilders

/**
  * @author Alex
  *         Created by Alex on 2018/9/11.
  */
object ElasticInsertWorker extends BaseWorker {

  override val prefix: String = "sync"

  def main(args: Array[String]): Unit = {
    System.setProperty("es.set.netty.runtime.available.processors", "false")

    val conf = sparkConf
    conf.set("cluster.name", ElasticFactory.clusterName)
    conf.set("es.nodes", ElasticFactory.address)
    conf.set("es.port", ElasticFactory.clientPort.toString)

    val sc = new SparkContext(conf)

    val indexName = configString("es.index")
    val typeName = configString("es.type")
    val batchSize = configInt("batchSize")

    esTest(indexName, typeName, "10000000")

  }

  def esTest(indexName: String, typeName: String, id: String): Unit = {
    val client = ElasticFactory.getESClient
    val r = client.prepareGet(indexName, typeName, id)
    println(r.get.getSource)
    val response = client.prepareSearch(indexName)
      .setTypes(typeName)
      .setQuery(QueryBuilders.matchQuery("str_0", "abc"))
      .setFrom(0).setSize(10).setExplain(true).get()

    println("response: " + response)
  }


}
