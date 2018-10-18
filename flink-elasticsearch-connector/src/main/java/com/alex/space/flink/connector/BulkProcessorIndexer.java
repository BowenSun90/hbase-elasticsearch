package com.alex.space.flink.connector;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

/**
 * @author Alex Created by Alex on 2018/9/14.
 */
public class BulkProcessorIndexer implements RequestIndexer {

  private final BulkProcessor bulkProcessor;

  public BulkProcessorIndexer(BulkProcessor bulkProcessor) {
    this.bulkProcessor = bulkProcessor;
  }

  @Override
  public void add(final ActionRequest... actionRequests) {
    for (ActionRequest actionRequest : actionRequests) {
      if (actionRequest instanceof IndexRequest) {
        this.bulkProcessor.add((IndexRequest) actionRequest);
      } else if (actionRequest instanceof DeleteRequest) {
        this.bulkProcessor.add((DeleteRequest) actionRequest);
      } else if (actionRequest instanceof UpdateRequest) {
        this.bulkProcessor.add((UpdateRequest) actionRequest);
      }
    }
  }
}
