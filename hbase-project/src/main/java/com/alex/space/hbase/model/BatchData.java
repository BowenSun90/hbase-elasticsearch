package com.alex.space.hbase.model;

import lombok.Builder;
import lombok.Getter;

/**
 * @author Alex Created by Alex on 2018/9/9.
 */
@Builder
@Getter
public class BatchData {

  /**
   * Rowkey
   */
  private final String rowKey;

  /**
   * Update column array
   */
  private final String[] columns;

  /**
   * Update value array
   */
  private final String[] values;

}
