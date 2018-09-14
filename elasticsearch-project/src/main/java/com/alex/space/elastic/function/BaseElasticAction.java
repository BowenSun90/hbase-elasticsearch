package com.alex.space.elastic.function;

/**
 * @author Alex Created by Alex on 2018/9/10.
 */
abstract class BaseElasticAction implements Runnable {

  String indexName;
  String typeName;
  int maxOffset;
  int insertSize;
  int batchSize;

  /**
   * 构造函数
   *
   * @param indexName 更新索引
   * @param typeName 更新类型
   * @param maxOffset 当前最大offset，即rowkey id
   * @param insertSize 插入数量
   * @param batchSize 批量插入数量
   */
  BaseElasticAction(String indexName, String typeName, int maxOffset, int insertSize,
      int batchSize) {
    this.indexName = indexName;
    this.typeName = typeName;
    this.maxOffset = maxOffset;
    this.insertSize = insertSize;
    this.batchSize = batchSize;
  }

}
