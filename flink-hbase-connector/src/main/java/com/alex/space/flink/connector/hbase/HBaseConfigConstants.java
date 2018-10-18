package com.alex.space.flink.connector.hbase;

/**
 * @author Alex Created by Alex on 2018/9/29.
 */
public interface HBaseConfigConstants {

  String HBASE_ZK_QUORUM = "hbase.zookeeper.quorum";

  String HBASE_ZK_PORT = "hbase.zookeeper.property.clientPort";

  String HBASE_ROOT_DIR = "hbase.rootdir";

  String HBASE_SCAN_TIMEOUT = "hbase.scanner.timeout.period";

  String HBASE_SCAN_BATCH = "hbase.scanner.batch";

  String DEFAULT_FS = "fs.defaultFS";

}
