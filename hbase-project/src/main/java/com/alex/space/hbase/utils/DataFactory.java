package com.alex.space.hbase.utils;

import com.alex.space.common.CommonConstants;
import com.alex.space.common.DataTypeEnum;
import com.alex.space.common.KeyValueGenerator;
import com.alex.space.hbase.config.HBaseConstants;
import com.alex.space.hbase.model.BatchData;
import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 生成随机更新数据
 *
 * @author Alex Created by Alex on 2018/9/10.
 */
public class DataFactory {

  /**
   * 生成随机数据
   *
   * @param offset 生成Rowkey的offset
   */
  public static BatchData generateBatchData(int offset) {
    // 每次随机插入列的数量，10～20随机
    int columnNum = ThreadLocalRandom.current().nextInt(10, 20);

    String[] columns = new String[columnNum];
    String[] values = new String[columnNum];

    for (int j = 0; j < columnNum - 1; j++) {
      // 随机生成数据类型，columnName和value
      DataTypeEnum dataTypeEnum = DataTypeEnum.randomType();

      columns[j] = KeyValueGenerator.randomKey(dataTypeEnum);
      values[j] = KeyValueGenerator.randomValue(dataTypeEnum);
    }
    columns[columnNum - 1] = HBaseConstants.DEFAULT_UPDATE_TIME;
    SimpleDateFormat sdf = new SimpleDateFormat(CommonConstants.DATE_FORMAT);
    values[columnNum - 1] = sdf.format(KeyValueGenerator.randomDateValue());

    // 生成Rowkey
    String rowKey = RowKeyUtils.buildNumberRowkey(offset);

    return BatchData.builder()
        .rowKey(rowKey)
        .columns(columns)
        .values(values)
        .build();
  }

  public static BatchData generateSelectData(int offset) {
    // 每次查询列的数量，5～10随机
    int columnNum = ThreadLocalRandom.current().nextInt(5, 10);

    String[] columns = new String[columnNum];

    for (int j = 0; j < columnNum; j++) {
      // 随机生成数据类型，columnName和value
      DataTypeEnum dataTypeEnum = DataTypeEnum.randomType();

      columns[j] = KeyValueGenerator.randomKey(dataTypeEnum);
    }

    // 生成Rowkey
    String rowKey = RowKeyUtils.buildNumberRowkey(offset);

    return BatchData.builder()
        .rowKey(rowKey)
        .columns(columns)
        .build();
  }

}
