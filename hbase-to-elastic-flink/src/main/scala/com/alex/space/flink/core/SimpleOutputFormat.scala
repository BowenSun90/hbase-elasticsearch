package com.alex.space.flink.core

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.slf4j.{Logger, LoggerFactory}

/**
  * DataSet output elasticsearch
  *
  * @author Alex Created by Alex on 2018/9/12.
  */
class SimpleOutputFormat extends OutputFormat[String] {

  val LOG: Logger = LoggerFactory.getLogger(classOf[SimpleOutputFormat])

  override def configure(configuration: Configuration): Unit = {
  }

  override def writeRecord(element: String): Unit = {
    LOG.info(element)
  }

  override def close(): Unit = {

  }

  override def open(i: Int, i1: Int): Unit = {

  }

}
