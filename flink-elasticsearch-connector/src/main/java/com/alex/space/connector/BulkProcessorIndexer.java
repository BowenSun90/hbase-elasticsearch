package com.alex.space.connector;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;

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
      }
    }
  }
}
