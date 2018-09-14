package com.alex.space.hbase.function;

import com.alex.space.hbase.utils.HBaseUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * Select from Hbase
 *
 * @author Alex Created by Alex on 2018/9/8.
 */
@Slf4j
public class HBaseSelect implements Runnable {

  private HBaseUtils hBaseUtils = HBaseUtils.getInstance();

  private int offset;

  public HBaseSelect(int offset) {
    this.offset = offset;
  }

  /**
   * 随机ID查询Hbase
   *
   * 随机查询字段
   */
  @Override
  public void run() {
      // TODO
  }
}
