package com.alex.space.hbase.utils;

import com.alex.space.common.CommonConstants;
import com.alex.space.common.DataTypeEnum;
import com.alex.space.common.KeyValueGenerator;
import com.alex.space.hbase.config.HBaseConstants;
import com.alex.space.hbase.model.BatchData;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 生成有一定业务含义的数据
 *
 * @author Alex Created by Alex on 2018/9/10.
 */
public class BizDataFactory {

  static Map<String, List<String>> keyMap = new HashMap<String, List<String>>() {
    {
      put(DataTypeEnum.String.name(), Arrays.asList("UID", "VipLevel", "Counter1", "TL1", "TL2"));
      put(DataTypeEnum.Number.name(), Arrays.asList("Tel", "TotalValues"));
      put(DataTypeEnum.Bool.name(), Arrays.asList("IsVip"));
      put(DataTypeEnum.Date.name(), Arrays.asList("RegisterDate"));
      put(DataTypeEnum.StringArray.name(), Arrays.asList("PurchaseBrand"));
      put(DataTypeEnum.NumberArray.name(), Arrays.asList("ProdQuantities"));
      put(DataTypeEnum.BoolArray.name(), Arrays.asList("BA"));
      put(DataTypeEnum.Json.name(), Arrays.asList("Address"));
    }
  };

  static Map<String, List<String>> valueMap = new HashMap<String, List<String>>() {
    {
      put("VipLevel", Arrays.asList("Gold", "Silver", "Bronze", "Platinum", ""));
      put("Counter1", Arrays.asList("{\"A\":1,\"B\":2 }}", "{\"A\":1,\"C\":3}", ""));
      put("TL1", Arrays.asList(
          "{\"2018-01-01\":[1,2,3],\"2018-01-02\":[5,6]}",
          "{\"2018-01-03\":[1,2,3]}", ""));
      put("TL2", Arrays.asList(
          "{\"2018-01-01\":[{\"A\":1,\"B\":2}],\"2018-01-02\":[{\"C\":3},{\"A\":4}]}",
          "{\"2018-01-03\":[{\"A\":1}],\"2018-01-04\":[{\"C\":3}]}", ""));
      put("Tel", Arrays.asList("13900000000", "13500000000", "13700000000", ""));
      put("TotalValues", Arrays.asList("1", "2", "3", "4", "5", "6", "7", ""));
      put("IsVip", Arrays.asList("True", "False", ""));
      put("RegisterDate", Arrays.asList("2018-01-01", "2018-01-02", "2018-01-03", ""));
      put("PurchaseBrand", Arrays.asList("BrandA", "BrandB", "BrandC", ""));
      put("ProdQuantities", Arrays.asList("100", "200", "300", "400", ""));
      put("Address", Arrays.asList(
          "{\"value\":\"北京市朝阳区265号\",\"province\":\"北京市\",\"city\":\"北京市\"}",
          "{\"value\":\"浙江省朝阳区265号\",\"province\":\"浙江省\",\"city\":\"杭州市\"}", ""));
      put("BA", Arrays.asList(""));

    }
  };

  private static String generateKey(DataTypeEnum dataTypeEnum) {
    List<String> keyList = keyMap.get(dataTypeEnum.name());
    return keyList.get(ThreadLocalRandom.current().nextInt(keyList.size()));
  }

  private static String generateValue(String key) {
    List<String> valueList = valueMap.getOrDefault(key, new ArrayList<>());
    return valueList.get(ThreadLocalRandom.current().nextInt(valueList.size()));
  }

  /**
   * 生成随机数据
   *
   * @param offset 生成Rowkey的offset
   */
  public static BatchData generateBatchData(int offset) {
    // 每次随机插入列的数量，10～20随机
    int columnNum = ThreadLocalRandom.current().nextInt(1, 4);

    String[] columns = new String[columnNum];
    String[] values = new String[columnNum];

    for (int j = 0; j < columnNum - 1; j++) {
      // 随机生成数据类型，columnName和value
      DataTypeEnum dataTypeEnum = DataTypeEnum.randomType();

      columns[j] = generateKey(dataTypeEnum);
      if (columns[j].equalsIgnoreCase("UID")) {
        values[j] = String.valueOf(offset);
      } else {
        values[j] = generateValue(columns[j]);
      }
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


}
