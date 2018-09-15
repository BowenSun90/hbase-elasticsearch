package com.alex.space.elastic.function;

import com.alex.space.common.DataTypeEnum;
import com.alex.space.common.KeyValueGenerator;
import com.alex.space.elastic.utils.ElasticUtils;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.StopWatch;

/**
 * ElasticSearch query code
 *
 * @author Alex Created by Alex on 2018/9/10.
 */
@Slf4j
public class ElasticQuery extends BaseElasticAction {

  public ElasticQuery(String indexName, String typeName, int offset, int insertSize,
      int batchSize) {
    super(indexName, typeName, offset, insertSize, batchSize);
  }

  @Override
  public void run() {

    StopWatch stopWatch = new StopWatch();

    int avgTime = 0;
    int optCount = 0;
    stopWatch.start();

    for (int i = 0; i < insertSize; i++) {

      try {
        DataTypeEnum dataTypeEnum = DataTypeEnum.randomType();

        String field = KeyValueGenerator.randomKey(dataTypeEnum);
        String value = KeyValueGenerator.randomValue(dataTypeEnum);
        String[] showFields = {
            "_id"
        };

        ElasticUtils.queryData(indexName, typeName, showFields, value, field);

        if (i % batchSize == 0 && i != 0) {
          stopWatch.stop();
          optCount++;

          // 计算平均插入时间
          avgTime += stopWatch.totalTime().getMillis();
          if (optCount % 50 == 0 && optCount != 0) {
            log.info("Avg query " + batchSize + " time: " + avgTime / 50.0 / 1000.0 + "s.");
            avgTime = 0;
          }
          stopWatch = new StopWatch();
          stopWatch.start();
        }
      } catch (Exception e) {
        log.debug(e.getMessage());
      }

    }

  }
}
