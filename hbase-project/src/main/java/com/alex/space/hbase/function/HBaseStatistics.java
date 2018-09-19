package com.alex.space.hbase.function;

import com.alex.space.hbase.hfile.ClientSideScanner;
import com.alex.space.hbase.utils.HBaseUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Alex Created by Alex on 2018/9/15.
 */
@Slf4j
public class HBaseStatistics {

  private String tableName;
  private String cf;

  public HBaseStatistics(String tableName, String cf) {
    this.tableName = tableName;
    this.cf = cf;
  }

  public void printStatistics() {
    try {

      HBaseUtils hBaseUtils = HBaseUtils.getInstance();
      hBaseUtils.printScan(tableName, cf);

      ClientSideScanner clientSideScanner = new ClientSideScanner();
      clientSideScanner.tableScan(tableName, cf);
    } catch (Exception e) {
      log.error(e.getMessage());
    }
  }

}
