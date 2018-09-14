package com.alex.space.connector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alex Created by Alex on 2018/9/14.
 */
public class ElasticSearchOutputFormat<T> extends RichOutputFormat<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchOutputFormat.class);

  /**
   * Elasticsearch配置
   */
  private final Configuration configuration;

  /**
   * TransportClient连接地址
   */
  private final List<InetSocketAddress> transportAddresses;

  /**
   * ElasticsearchSinkFunction
   */
  private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

  /**
   * Elasticsearch client
   */
  private transient Client client;

  /**
   * Bulk processor
   */
  private transient BulkProcessor bulkProcessor;

  /**
   * Bulk {@link org.elasticsearch.action.ActionRequest} indexer
   */
  private transient RequestIndexer requestIndexer;

  private final AtomicBoolean hasFailure = new AtomicBoolean(false);

  /**
   * This is set from inside the BulkProcessor listener if a Throwable was thrown during
   * processing.
   */
  private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();


  public ElasticSearchOutputFormat(Map<String, String> userConfig,
      List<InetSocketAddress> transportAddresses,
      ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {

    this.transportAddresses = transportAddresses;
    Preconditions.checkArgument(transportAddresses != null && transportAddresses.size() > 0);
    this.elasticsearchSinkFunction = elasticsearchSinkFunction;

    configuration = new Configuration();
    userConfig.entrySet().forEach(e -> {
      configuration.setString(e.getKey(), e.getValue());
    });
  }


  @Override
  public void configure(final Configuration parameters) {
    System.setProperty("es.set.netty.runtime.available.processors", "false");

    Settings settings = Settings.builder().put("cluster.name", "wifianalytics.es").build();

    TransportClient transportClient = new PreBuiltTransportClient(settings);

    for (InetSocketAddress address : transportAddresses) {
      transportClient.addTransportAddress(new InetSocketTransportAddress(address));
    }

    client = transportClient;

    BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(client,
        new Listener() {
          @Override
          public void beforeBulk(final long executionId, final BulkRequest request) {
            LOG.info("before Bulk, {}", executionId);
          }

          @Override
          public void afterBulk(final long executionId, final BulkRequest request,
              final BulkResponse response) {
            if (response.hasFailures()) {
              for (BulkItemResponse resp : response.getItems()) {
                if (resp.isFailed()) {
                  LOG.error(
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
            LOG.error(failure.getMessage());
            failureThrowable.compareAndSet(null, failure);
            hasFailure.set(true);
          }
        });

    // This makes flush() blocking
    bulkProcessorBuilder.setConcurrentRequests(0);

    bulkProcessorBuilder.setBulkActions(
        stringToInt(configuration
            .getString(ElasticConfigConstants.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "100")));

    bulkProcessorBuilder.setBulkSize(new ByteSizeValue(
        stringToInt(configuration
            .getString(ElasticConfigConstants.CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB, "1024"))));

    bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(
        stringToInt(configuration
            .getString(ElasticConfigConstants.CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, "5000"))));

    bulkProcessor = bulkProcessorBuilder.build();
    requestIndexer = new BulkProcessorIndexer(bulkProcessor);
  }

  @Override
  public void open(final int taskNumber, final int numTasks) throws IOException {
    LOG.info("open, {}", taskNumber);
  }

  @Override
  public void writeRecord(final T element) throws IOException {
    elasticsearchSinkFunction.process(element, getRuntimeContext(), requestIndexer);
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

  private int stringToInt(String str) throws NumberFormatException {
    return Integer.valueOf(str);
  }
}
