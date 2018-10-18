package com.alex.space.flink.hbase;

import java.io.Serializable;
import java.util.Map;
import lombok.ToString;

/**
 * RowData
 *
 * row entity
 *
 * @author Alex Created by Alex on 2018/9/19.
 */
@ToString
public class RowData implements Serializable {

  private String row;

  private Map<String, String> keyValue;

  public String getRow() {
    return row;
  }

  public void setRow(final String row) {
    this.row = row;
  }

  public Map<String, String> getKeyValue() {
    return keyValue;
  }

  public void setKeyValue(final Map<String, String> keyValue) {
    this.keyValue = keyValue;
  }

}
