package com.alex.space.hbase.utils;

import lombok.extern.slf4j.Slf4j;

/**
 * Rowkey utils
 *
 * @author Alex Created by Alex on 2018/9/8.
 */
@Slf4j
public class RowKeyUtils {

  /**
   * 自增数字做Rowkey，用0补齐8位
   *
   * 为了适应预分区，反转字符串
   *
   * @param num 自增ID
   * @return rowkey
   */
  public static String buildNumberRowkey(int num) {
    String rowkey = String.format("%08d", num);
    StringBuilder sb = new StringBuilder(rowkey);
    rowkey = sb.reverse().toString();

    return rowkey;
  }

}
