package com.alex.space.hbase.function;

import com.alex.space.hbase.utils.HBaseUtils;

/**
 * @author Alex Created by Alex on 2018/9/9.
 */
public abstract class BaseHBaseAction implements Runnable {

  HBaseUtils hBaseUtils = HBaseUtils.getInstance();

  String tableName;
  String cf;
  int maxOffset;
  int insertSize;
  int batchSize;
  int maxColNum;
  int minColNum;

  /**
   * 构造函数
   *
   * @param tableName 插入表名
   * @param cf 插入列族
   * @param maxOffset 插入起始offset，即rowkey id
   * @param insertSize 插入数量
   * @param batchSize 批量插入数量
   */
  BaseHBaseAction(String tableName, String cf, int maxOffset, int insertSize,
      int batchSize, int maxColNum, int minColNum) {
    this.tableName = tableName;
    this.cf = cf;
    this.maxOffset = maxOffset;
    this.insertSize = insertSize;
    this.batchSize = batchSize;
    this.maxColNum = maxColNum;
    this.minColNum = minColNum;
  }


}
