package com.alex.space.flink.connector;

/**
 * ElasticsearchConfigConstants
 *
 * @author Alex Created by Alex on 2018/9/29.
 */
public interface ElasticsearchConfigConstants {

  String ES_CLUSTER_NAME = "es.cluster.name";

  String ES_NODE_IPS = "es.node.ips";

  String ES_NODE_PORT = "es.node.port";

  String ES_BULK_ACTIONS = "es.bulk.actions";

  String ES_BULK_SIZE_KB = "es.bulk.size.kb";

  String ES_BULK_FLUSH_INTERVAL_MS = "es.bulk.flush.interval";

  String ES_BULK_FLUSH_BACKOFF_ENABLE = "es.bulk.flush.backoff.enable";

  String ES_BULK_FLUSH_BACKOFF_TYPE = "es.bulk.flush.backoff.type";

  String ES_BULK_FLUSH_BACKOFF_RETRIES = "es.bulk.flush.backoff.retries";

  String ES_BULK_FLUSH_BACKOFF_DELAY = "es.bulk.flush.backoff.delay";

}
