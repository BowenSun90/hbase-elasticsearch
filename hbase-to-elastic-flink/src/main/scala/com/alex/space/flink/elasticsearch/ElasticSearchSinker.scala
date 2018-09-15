package com.alex.space.flink.elasticsearch

import org.apache.flink.api.common.functions.RuntimeContext
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory


/**
  * @author Alex
  *         Created by Alex on 2018/9/14.
  */
class ElasticSearchSinker(val indexName: String, val typeName: String) extends ElasticsearchSinkFunction[(String, String)] {

  private val LOG = LoggerFactory.getLogger(classOf[ElasticSearchSinker])

  def createIndexRequest(element: (String, String)): IndexRequest = {
    LOG.debug("create index request " + element)
    Requests.indexRequest
      .index(indexName)
      .`type`(typeName)
      .id(element._1)
      .source(element._2, XContentType.JSON)
  }

  override def process(element: (String, String), ctx: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    requestIndexer.add(createIndexRequest(element))
  }

}

