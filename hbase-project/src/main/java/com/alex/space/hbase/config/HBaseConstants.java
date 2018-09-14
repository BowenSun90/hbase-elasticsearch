package com.alex.space.hbase.config;

/**
 * HBase constants
 *
 * @author Alex Created by Alex on 2018/9/8.
 */
public interface HBaseConstants {

  /**
   * HBase zookeeper quorum
   */
  String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

  /**
   * HBase zookeeper client
   */
  String ZOOKEEPER_PORT = "hbase.zookeeper.property.clientPort";

  /**
   * HBase默认添加列，最近更新时间
   */
  String DEFAULT_UPDATE_TIME = "lastModifyDate";
}
