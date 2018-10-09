package com.alex.space.hbase.utils;

import com.alex.space.common.CommonConstants;
import com.alex.space.common.DataTypeEnum;
import com.alex.space.common.KeyValueGenerator;
import com.alex.space.hbase.config.HBaseConstants;
import com.alex.space.hbase.model.BatchData;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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

  private static Map<String, List<String>> keyMap = new HashMap<String, List<String>>() {
    {
      put(DataTypeEnum.String.name(), Arrays.asList("UID", "VipLevel", "Counter1", "TL1", "TL2"));
      put(DataTypeEnum.Number.name(), Arrays.asList("Tel", "TotalValues"));
      put(DataTypeEnum.Bool.name(), Arrays.asList("IsVip", "Man"));
      put(DataTypeEnum.Date.name(), Arrays.asList("RegisterDate", "LoginDate"));
      put(DataTypeEnum.StringArray.name(), Arrays.asList("PurchaseBrand", "ProdName"));
      put(DataTypeEnum.NumberArray.name(), Arrays.asList("ProdQuantities", "Quantities"));
      put(DataTypeEnum.BoolArray.name(), Arrays.asList("BA", "BA2"));
      put(DataTypeEnum.Json.name(), Arrays.asList("Address", "Add2"));
    }
  };

  private static Map<String, List<String>> valueMap = new HashMap<String, List<String>>() {
    {
      put("VipLevel", Arrays.asList("",
          "Gold", "Silver", "Bronze", "Platinum"));
      put("Counter1", Arrays.asList("",
          "{\"A\":1,\"B\":2}",
          "{\"A\":1,\"C\":3}",
          "{\"A\":1,\"B\":2,\"C\":3}",
          "{\"A\":1,\"B\":2,\"C\":3, \"D\":4}",
          "{\"B\":2,\"C\":3, \"D\":4}",
          "{\"D\":4,\"E\":5}"));
      put("TL1", Arrays.asList("",
          "{\"2018-09-10\":[1,2],\"2018-09-20\":[3,4]}",
          "{\"2018-09-20\":[3,4],\"2018-09-30\":[5,6]}",
          "{\"2018-09-10\":[1,2],\"2018-09-20\":[3,4],\"2018-09-30\":[5,6]}",
          "{\"2018-09-30\":[5,6]}",
          "{\"2018-09-30\":[5,6], \"2018-10-07\": [105, 115]}",
          "{\"2018-09-30\":[5,6],\"2018-10-07\":[105,115],\"2018-10-09\":[105,1150]}"));
      put("TL2", Arrays.asList("",
          "{\"2018-09-01\":[{\"A\":1}],\"2018-09-20\":[{\"C\":3,\"D\":4}],\"2018-09-30\":[{\"A\":5,\"C\":6}]}",
          "{\"2018-09-01\":[{\"A\":1,\"B\":2}],\"2018-09-20\":[{\"C\":3,\"D\":4}]}",
          "{\"2018-09-20\":[{\"C\":3,\"D\":4}],\"2018-09-30\":[{\"A\":5,\"C\":6}]}",
          "{\"2018-09-30\":[{\"A\":5,\"C\":6}]}",
          "{\"2018-09-30\":[{\"A\":5,\"C\":6}],\"2018-10-07\":[{\"B\":5,\"E\":105}]}",
          "{\"2018-10-07\":[{\"B\":5}],\"2018-10-09\":[{\"A\":5,\"E\":105},{\"B\":5,\"A\":105}]}"));
      put("Tel", Arrays.asList("",
          "13900000000", "13500000000", "13700000000", "18500000000", "13100000000",
          "13300000000", "18800000000", "17300000000", "18300000000", "13000000000"));
      put("TotalValues", Arrays.asList("",
          "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "100", "200", "300", "400", "500"));
      put("IsVip", Arrays.asList("",
          "true", "false"));
      put("Man", Arrays.asList("",
          "true", "false"));
      put("RegisterDate", Arrays.asList("",
          "2018-01-01", "2018-01-02", "2018-01-03", "2018-01-04",
          "2018-09-01", "2018-09-10", "2018-09-20", "2018-09-30",
          "2018-10-01", "2018-10-02", "2018-10-03", curDate()));
      put("LoginDate", Arrays.asList("",
          "2018-01-01", "2018-01-02", "2018-01-03", "2018-01-04",
          "2018-09-01", "2018-09-10", "2018-09-20", "2018-09-30",
          "2018-10-01", "2018-10-02", "2018-10-03", curDate()));
      put("PurchaseBrand", Arrays.asList("", "[]",
          "[\"BrandA\",\"BrandB\"]",
          "[\"BrandB\"]",
          "[\"BrandC\",\"BrandA\"]",
          "[\"BrandD\"]",
          "[\"BrandE\",\"BrandA\"]",
          "[\"BrandE\",\"BrandF\"]"));
      put("ProdName", Arrays.asList("", "[]",
          "[\"AAA\",\"BBB\"]",
          "[\"AAA\"]",
          "[\"AAA\",\"CCC\"]",
          "[\"BBB\"]",
          "[\"CCC\",\"AAA\"]",
          "[\"BBB\",\"CCC\"]"));
      put("ProdQuantities", Arrays.asList("", "[]",
          "[100, 200]", "[200,300]", "[300]", "[400,500,600]", "[10,105,11]", "[105,0]"));
      put("Quantities", Arrays.asList("", "[]",
          "[100, 200]", "[200,300]", "[300]", "[400,500,600]", "[10,105,11]", "[105,0]"));
      put("Address", Arrays.asList("", "{}",
          "{\"value\":\"北京市朝阳区265号\",\"province\":\"北京市\",\"city\":\"北京市\"}",
          "{\"value\":\"浙江省杭州市朝阳区265号\",\"province\":\"浙江省\",\"city\":\"杭州市\"}",
          "{\"value\":\"辽宁省朝阳市西城区265号\",\"province\":\"辽宁省\",\"city\":\"朝阳市\"}"));
      put("Add2", Arrays.asList("", "{}",
          "{\"value\":\"北京市朝阳区265号\",\"province\":\"北京市\",\"city\":\"北京市\"}",
          "{\"value\":\"浙江省杭州市朝阳区265号\",\"province\":\"浙江省\",\"city\":\"杭州市\"}",
          "{\"value\":\"辽宁省朝阳市西城区265号\",\"province\":\"辽宁省\",\"city\":\"朝阳市\"}"));
      put("BA", Arrays.asList("", "[]",
          "[true]", "[true,false]"));
      put("BA2", Arrays.asList("", "[]",
          "[true]", "[true,false]"));
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

  private static String curDate() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    return sdf.format(new Date());
  }

  /**
   * 生成随机数据
   *
   * @param offset 生成Rowkey的offset
   */
  public static BatchData generateBatchData(int offset) {
    // 每次随机插入列的数量，10～20随机
    int columnNum = ThreadLocalRandom.current().nextInt(1, 5);

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
