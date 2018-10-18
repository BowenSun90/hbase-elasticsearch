package com.alex.space.flink.connector;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * ElasticsearchOutputFormat
 *
 * @author Alex Created by Alex on 2018/9/29.
 */
@Slf4j
public class ElasticsearchOutputFormat<T> extends RichOutputFormat<T> {

  private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

  private Client client;

  private BulkProcessor bulkProcessor;

  private RequestIndexer requestIndexer;

  private AtomicBoolean hasFailure = new AtomicBoolean(false);

  private AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

  public ElasticsearchOutputFormat(ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
    this.elasticsearchSinkFunction = elasticsearchSinkFunction;
  }

  @Override
  public void configure(final Configuration parameters) {
    System.setProperty("es.set.netty.runtime.available.processors", "false");

    String cluster = parameters.getString(
        ElasticsearchConfigConstants.ES_CLUSTER_NAME, "wifianalytics.es");
    String nodes = parameters.getString(
        ElasticsearchConfigConstants.ES_NODE_IPS, "");
    int port = parameters.getInteger(
        ElasticsearchConfigConstants.ES_NODE_PORT, 9300);

    Settings settings = Settings.builder().put("cluster.name", cluster).build();
    TransportAddress[] endpoints = Arrays.stream(nodes.split(","))
        .map(ip -> ip.split(":", -1))
        .map(ips -> {
          try {
            if (ips.length == 1) {
              return new TransportAddress(InetAddress.getByName(ips[0]), port);
            } else {
              return new TransportAddress(InetAddress.getByName(ips[0]), Integer.parseInt(ips[1]));
            }
          } catch (Exception e) {
            log.error("Create TransportAddress with exception", e);
          }
          return null;
        })
        .filter(Objects::nonNull)
        .toArray(TransportAddress[]::new);

    client = new PreBuiltTransportClient(settings).addTransportAddresses(endpoints);

    BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor
        .builder(client, new BulkProcessor.Listener() {
          @Override
          public void beforeBulk(final long executionId, final BulkRequest request) {

          }

          @Override
          public void afterBulk(final long executionId, final BulkRequest request,
              final BulkResponse response) {
            log.warn("Bulk process end, items: {}, time: {}",
                response.getItems().length, response.getTook());

            if (response.hasFailures()) {
              for (BulkItemResponse resp : response.getItems()) {
                if (resp.isFailed()) {
                  log.error(
                      "Failed to index document in Elasticsearch: " + resp.getFailureMessage());
                  failureThrowable
                      .compareAndSet(null, new RuntimeException(resp.getFailureMessage()));

                }
              }
              hasFailure.set(true);
            }
          }

          @Override
          public void afterBulk(final long executionId, final BulkRequest request,
              final Throwable failure) {
            log.error("Failed Elasticsearch bulk request: {}.", failure.getMessage());
            failureThrowable.compareAndSet(null, failure);
            hasFailure.set(true);
          }
        });

    // This makes flush() blocking
    bulkProcessorBuilder.setConcurrentRequests(0);

    int bulkProcessorFlushMaxActions = parameters
        .getInteger(ElasticsearchConfigConstants.ES_BULK_ACTIONS, 2000);
    int bulkProcessorFlushMaxSizeKB = parameters
        .getInteger(ElasticsearchConfigConstants.ES_BULK_SIZE_KB, 10240);
    int bulkProcessorFlushIntervalMillis = parameters
        .getInteger(ElasticsearchConfigConstants.ES_BULK_FLUSH_INTERVAL_MS, 15000);

    bulkProcessorBuilder.setBulkActions(bulkProcessorFlushMaxActions);
    bulkProcessorBuilder
        .setBulkSize(new ByteSizeValue(bulkProcessorFlushMaxSizeKB, ByteSizeUnit.KB));
    bulkProcessorBuilder
        .setFlushInterval(TimeValue.timeValueMillis(bulkProcessorFlushIntervalMillis));

    bulkProcessor = bulkProcessorBuilder.build();
    requestIndexer = new BulkProcessorIndexer(bulkProcessor);

  }

  @Override
  public void open(final int taskNumber, final int numTasks) throws IOException {
    log.info("ElasticsearchOutputFormat open, taskNumber {}.", taskNumber);
  }

  @Override
  public void writeRecord(final T record) throws IOException {
    elasticsearchSinkFunction.process(record, getRuntimeContext(), requestIndexer);
  }

  @Override
  public void close() throws IOException {
    if (bulkProcessor != null) {
      bulkProcessor.close();
      bulkProcessor = null;
    }
    if (client != null) {
      client.close();
      client = null;
    }
    if (hasFailure.get()) {
      Throwable cause = failureThrowable.get();
      if (cause != null) {
        throw new RuntimeException("An error in ElasticSearchOutputFormat.", cause);
      } else {
        throw new RuntimeException("An error in ElasticSearchOutputFormat.");
      }
    }
  }
}
