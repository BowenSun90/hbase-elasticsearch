package com.alex.space.elastic.utils;

import com.alex.space.common.DataTypeEnum;
import com.alex.space.common.KeyValueGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * 生成随机文档
 *
 * @author Alex Created by Alex on 2018/5/24.
 */
@Slf4j
public class DataFactory {

  /**
   * 生成批量文档
   *
   * @param batchSize batch size
   */
  public static List<XContentBuilder> generateDocumentList(int batchSize) {

    List<XContentBuilder> list = new ArrayList<>();

    for (int i = 0; i < batchSize; i++) {
      try {
        list.add(generateRandomDoc());
      } catch (Exception e) {
        log.error("Generate random json with exception: {}", e.getMessage());
      }
    }

    return list;
  }

  /**
   * 生成随机文档
   */
  private static XContentBuilder generateRandomDoc() throws IOException {
    // 每次随机插入列的数量，10～20随机
    int columnNum = ThreadLocalRandom.current().nextInt(10, 20);

    XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
    XContentBuilder builder = jsonBuild.startObject();

    for (int i = 0; i < columnNum; i++) {
      DataTypeEnum dataTypeEnum = DataTypeEnum.randomType();

      if (dataTypeEnum == DataTypeEnum.StringArray) {

        builder.array(KeyValueGenerator.randomKey(dataTypeEnum),
            KeyValueGenerator.randomStringArray());

      } else if (dataTypeEnum == DataTypeEnum.NumberArray) {

        builder.array(KeyValueGenerator.randomKey(dataTypeEnum),
            KeyValueGenerator.randomNumberArray());

      } else if (dataTypeEnum == DataTypeEnum.BoolArray) {

        builder.array(KeyValueGenerator.randomKey(dataTypeEnum),
            KeyValueGenerator.randomBoolArray());

      } else if (dataTypeEnum == DataTypeEnum.String) {

        builder.field(KeyValueGenerator.randomKey(dataTypeEnum),
            KeyValueGenerator.randomStringValue(5));

      } else if (dataTypeEnum == DataTypeEnum.Number) {

        builder.field(KeyValueGenerator.randomKey(dataTypeEnum),
            KeyValueGenerator.randomNumberValue());

      } else if (dataTypeEnum == DataTypeEnum.Bool) {

        builder.field(KeyValueGenerator.randomKey(dataTypeEnum),
            KeyValueGenerator.randomBoolValue());

      } else if (dataTypeEnum == DataTypeEnum.Date) {

        builder.field(KeyValueGenerator.randomKey(dataTypeEnum),
            KeyValueGenerator.randomDateValue());

      } else if (dataTypeEnum == DataTypeEnum.Json) {

        builder.startObject(KeyValueGenerator.randomKey(dataTypeEnum))
            .field("str", KeyValueGenerator.randomStringValue(5))
            .field("date", KeyValueGenerator.randomDateValue())
            .field("msg", KeyValueGenerator.randomStringValue(10))
            .endObject();

      }

    }
    builder.endObject();

    return jsonBuild;
  }

}
