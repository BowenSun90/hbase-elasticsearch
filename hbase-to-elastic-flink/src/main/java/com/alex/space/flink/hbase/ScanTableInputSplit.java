package com.alex.space.flink.hbase;

import org.apache.flink.core.io.LocatableInputSplit;

/**
 * @author Alex Created by Alex on 2018/9/19.
 */
public class ScanTableInputSplit extends LocatableInputSplit {

  private static final long serialVersionUID = 1L;

  /**
   * The name of the table to retrieve data from.
   */
  private final byte[] tableName;

  /**
   * The region name of the split.
   */
  private final String regionName;

  /**
   * Creates a new table input split.
   *
   * @param splitNumber the number of the input split
   * @param hostnames the names of the hosts storing the data the input split refers to
   * @param tableName the name of the table to retrieve data from
   * @param regionName the region name of the split
   */
  ScanTableInputSplit(final int splitNumber, final String[] hostnames,
      final byte[] tableName,
      final String regionName) {
    super(splitNumber, hostnames);

    this.tableName = tableName;
    this.regionName = regionName;
  }

  /**
   * Returns the table name.
   *
   * @return The table name.
   */
  public byte[] getTableName() {
    return this.tableName;
  }

  /**
   * Returns the region name.
   *
   * @return The region name.
   */
  public String getRegionName() {
    return this.regionName;
  }
}
