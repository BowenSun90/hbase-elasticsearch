package com.alex.space.flink.elasticsearch

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest

/**
  * @author Alex Created by Alex on 2018/9/14.
  */
class BulkProcessorIndexer(val bulkProcessor: BulkProcessor) extends RequestIndexer {
  override def add(actionRequests: ActionRequest*): Unit = {
    for (actionRequest <- actionRequests) {

      actionRequest match {
        case request: IndexRequest => this.bulkProcessor.add(request)
        case request: DeleteRequest => this.bulkProcessor.add(request)
        case _ =>
      }

    }
  }
}
