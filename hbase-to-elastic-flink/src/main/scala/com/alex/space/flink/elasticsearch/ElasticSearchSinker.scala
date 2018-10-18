package com.alex.space.flink.elasticsearch

import com.alex.space.flink.connector.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.api.common.functions.RuntimeContext
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.xcontent.XContentBuilder


/**
  * @author Alex
  *         Created by Alex on 2018/9/14.
  */
class ElasticSearchSinker(val indexName: String, val typeName: String)
  extends ElasticsearchSinkFunction[(String, XContentBuilder)] {

  override def process(element: (String, XContentBuilder), ctx: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    requestIndexer.add(createUpdateRequest(element))
  }

  def createUpdateRequest(element: (String, XContentBuilder)): UpdateRequest = {
    val updateRequest = new UpdateRequest
    updateRequest.index(indexName)
      .`type`(typeName)
      .id(element._1)
      .doc(element._2)
      .docAsUpsert(true)

    updateRequest
  }
}

