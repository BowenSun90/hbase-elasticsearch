package com.alex.space.flink.elasticsearch

import java.io.IOException
import java.net.InetAddress
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import com.alex.space.flink.config.Configable
import com.typesafe.config.Config
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration
import org.elasticsearch.action.bulk.{BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.{ByteSizeValue, TimeValue}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.LoggerFactory

/**
  * @author Alex
  *         Created by Alex on 2018/9/14.
  */
object ElasticSearchOutputFormat {

  private val LOG = LoggerFactory.getLogger(classOf[ElasticSearchOutputFormat[_]])

}

class ElasticSearchOutputFormat[T](val elasticsearchSinkFunction: ElasticsearchSinkFunction[T])
  extends RichOutputFormat[T] with Configable {

  override val prefix: String = "es"

  /**
    * Elasticsearch client
    */
  private var client: Client = _

  /**
    * Bulk processor
    */
  private var bulkProcessor: BulkProcessor = _

  /**
    * Bulk request indexer
    */
  private var requestIndexer: RequestIndexer = _

  final private val hasFailure: AtomicBoolean = new AtomicBoolean(false)

  final private val failureThrowable: AtomicReference[Throwable] = new AtomicReference[Throwable]

  override def configure(parameters: Configuration): Unit = {
    System.setProperty("es.set.netty.runtime.available.processors", "false")

    val settings = Settings.builder().put("cluster.name", configString("cluster.name")).build()
    val nodes = configString("node.ips")
    val endpoints = nodes.split(",").map(_.split(":", -1))
      .map {
        case Array(host, port) => new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)
        case Array(host) => new InetSocketTransportAddress(InetAddress.getByName(host), configInt("node.port"))
      }

    client = new PreBuiltTransportClient(settings).addTransportAddresses(endpoints: _*)

    val bulkProcessorBuilder: BulkProcessor.Builder = BulkProcessor.builder(client, new BulkProcessor.Listener() {
      override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
        ElasticSearchOutputFormat.LOG.info("before Bulk, {}", executionId)
      }

      override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
        if (response.hasFailures) {
          for (resp <- response.getItems) {
            if (resp.isFailed) {
              ElasticSearchOutputFormat.LOG.error("Failed to index document in Elasticsearch: " + resp.getFailureMessage)
              failureThrowable.compareAndSet(null, new RuntimeException(resp.getFailureMessage))
            }
          }
          hasFailure.set(true)
        }
      }

      override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
        ElasticSearchOutputFormat.LOG.error(failure.getMessage)
        failureThrowable.compareAndSet(null, failure)
        hasFailure.set(true)
      }
    })

    // This makes flush() blocking
    bulkProcessorBuilder.setConcurrentRequests(0)

    bulkProcessorBuilder.setBulkActions(500)
    bulkProcessorBuilder.setBulkSize(new ByteSizeValue(1024))
    bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(5000))

    bulkProcessor = bulkProcessorBuilder.build
    requestIndexer = new BulkProcessorIndexer(bulkProcessor)

    ElasticSearchOutputFormat.LOG.info("hello")
  }

  @throws[IOException]
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    ElasticSearchOutputFormat.LOG.info("open, {}", taskNumber)
  }

  @throws[IOException]
  override def writeRecord(element: T): Unit = {
    elasticsearchSinkFunction.process(element, getRuntimeContext, requestIndexer)
  }

  @throws[IOException]
  override def close(): Unit = {
    if (bulkProcessor != null) {
      bulkProcessor.close()
      bulkProcessor = null
    }
    if (client != null) {
      client.close()
      client = null
    }
    if (hasFailure.get) {
      val cause: Throwable = failureThrowable.get
      if (cause != null) {
        throw new RuntimeException("An error in ElasticSearchOutputFormat.", cause)
      }
      else {
        throw new RuntimeException("An error in ElasticSearchOutputFormat.")
      }
    }
  }

}
