package com.alex.space.flink.elasticsearch

import java.io.Serializable

import org.apache.flink.api.common.functions.{Function, RuntimeContext}

/**
  * @author Alex Created by Alex on 2018/9/14.
  */
trait ElasticsearchSinkFunction[T] extends Serializable with Function {
  def process(element: T, ctx: RuntimeContext, requestIndexer: RequestIndexer): Unit
}
