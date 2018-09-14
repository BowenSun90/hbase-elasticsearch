package com.alex.space.flink.elasticsearch

import java.io.Serializable

import org.elasticsearch.action.ActionRequest

/**
  * @author Alex Created by Alex on 2018/9/14.
  */
trait RequestIndexer extends Serializable {
  def add(actionRequests: ActionRequest*): Unit
}
