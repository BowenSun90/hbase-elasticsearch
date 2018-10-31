package com.alex.space.common;

/**
 * Common constants
 *
 * @author Alex Created by Alex on 2018/9/11.
 */
public interface CommonConstants {

//  int MAX_OFFSET = 99999999;
//  String ROWKEY_FORMAT = "%08d";


  /**
   * Max offset
   */
  int MAX_OFFSET = 999999999;

  /**
   * Rowkey format
   */
  String ROWKEY_FORMAT = "%09d";

  /**
   * 同一类型的列数量
   */
  int COLUMN_MAX_NUMBER = 100;

  /**
   * Json类型的列数量
   */
  int JSON_MAX_NUMBER = 50;

  /**
   * Date format
   */
  String DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";

}
