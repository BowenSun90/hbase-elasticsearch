package com.alex.space.hbase.function;

import com.alex.space.hbase.model.BatchData;
import com.alex.space.hbase.utils.BizDataFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Insert into Hbase
 *
 * @author Alex Created by Alex on 2018/9/8.
 */
@Slf4j
public class HBaseInsertBiz extends HBaseInsert {

  public HBaseInsertBiz(String tableName, String cf, int offset, int insertSize, int batchSize,
      int maxColNum, int minColNum) {
    super(tableName, cf, offset, insertSize, batchSize, maxColNum, minColNum);
  }

  /**
   * 创建有一定业务含义的数据
   *
   * @param offset offset
   */
  @Override
  protected BatchData generateBatchData(int offset) {
    return BizDataFactory.generateBatchData(offset);
  }
}
