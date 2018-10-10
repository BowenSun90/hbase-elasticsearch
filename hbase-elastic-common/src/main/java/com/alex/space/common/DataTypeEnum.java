package com.alex.space.common;

import java.util.concurrent.ThreadLocalRandom;
import lombok.Getter;

/**
 * Data type enum
 *
 * @author Alex Created by Alex on 2018/9/9.
 */
@Getter
public enum DataTypeEnum {

  //
  String("str_", "keyword", false),
  Number("num_", "double", false),
  Date("date_", "date", false),
  Bool("bool_", "boolean", false),
  StringArray("str_array_", "keyword", true),
  NumberArray("num_array_", "double", true),
  BoolArray("bool_array_", "boolean", true),
  Json("json_", "object", false);

  private String keyName;
  private String esType;
  private boolean isArray;

  DataTypeEnum(String keyName, String esType, boolean isArray) {
    this.keyName = keyName;
    this.esType = esType;
    this.isArray = isArray;
  }

  public static DataTypeEnum randomType() {
    int length = DataTypeEnum.values().length;
    return DataTypeEnum.values()[ThreadLocalRandom.current().nextInt(length)];
  }

}
