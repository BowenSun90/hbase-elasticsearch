package com.alex.space.flink.elasticsearch

import java.io.IOException
import java.net.InetAddress
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import com.alex.space.flink.config.Configable
import com.google.common.util.concurrent.AtomicDouble
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration
import org.elasticsearch.action.bulk.{BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author Alex
  *         Created by Alex on 2018/9/14.
  */
class ElasticSearchOutputFormat[T](val batchSize: Int, val elasticsearchSinkFunction: ElasticsearchSinkFunction[T])
  extends RichOutputFormat[T] with Configable {

  val LOG: Logger = LoggerFactory.getLogger(classOf[ElasticSearchOutputFormat[_]])

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

  val counter: AtomicInteger = new AtomicInteger()
  val took: AtomicDouble = new AtomicDouble()

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
        LOG.debug("before Bulk, {}", executionId)
      }

      override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
        if (response.hasFailures) {
          for (resp <- response.getItems) {
            if (resp.isFailed) {
              LOG.error("Failed to index document in Elasticsearch: " + resp.getFailureMessage)
              failureThrowable.compareAndSet(null, new RuntimeException(resp.getFailureMessage))
            }
          }
          hasFailure.set(true)
        }
        val c = counter.incrementAndGet()
        val t = took.addAndGet(response.getTookInMillis / 1000.0)

        if (c % 20 == 0 && c != 0) {
          LOG.info("Avg bulk took {}s", c / t)
          counter.set(0)
          took.set(0)
        }
      }

      override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
        LOG.error(failure.getMessage)
        failureThrowable.compareAndSet(null, failure)
        hasFailure.set(true)
      }
    })

    // This makes flush() blocking
    bulkProcessorBuilder.setConcurrentRequests(0)

    bulkProcessorBuilder.setBulkActions(batchSize)
    bulkProcessorBuilder.setBulkSize(new ByteSizeValue(1024, ByteSizeUnit.KB))
    bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(10000))

    bulkProcessor = bulkProcessorBuilder.build
    requestIndexer = new BulkProcessorIndexer(bulkProcessor)
  }

  @throws[IOException]
  override def open(taskNumber: Int, numTasks: Int): Unit = {

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
