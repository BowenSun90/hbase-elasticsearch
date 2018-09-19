package com.alex.space.flink.hbase;

import java.io.Serializable;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.hbase.Cell;

/**
 * @author Alex Created by Alex on 2018/9/19.
 */
@Getter
@Setter
@ToString
public class RowData implements Serializable {

  private String row;

  private Map<String, String> keyValue;

  private Cell[] cells;

}
