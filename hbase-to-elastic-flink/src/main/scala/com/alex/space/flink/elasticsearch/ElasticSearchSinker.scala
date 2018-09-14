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
class ElasticSearchSinker[T] extends ElasticsearchSinkFunction[T] {

  private val LOG = LoggerFactory.getLogger(classOf[ElasticSearchSinker[_]])

  def createIndexRequest(element: T): IndexRequest = {
    LOG.info("create index request " + element)
    Requests.indexRequest
      .index("test_table2")
      .`type`("d")
      .id(element.toString)
      .source("{\"age\": \"200\"}", XContentType.JSON)

  }

  override def process(element: T, ctx: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    requestIndexer.add(createIndexRequest(element))
  }

}

