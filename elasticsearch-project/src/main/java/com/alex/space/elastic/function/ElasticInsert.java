package com.alex.space.elastic.function;

import com.alex.space.elastic.utils.DataFactory;
import com.alex.space.elastic.utils.ElasticUtils;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.StopWatch;

/**
 * ElasticSearch insert code
 *
 * @author Alex Created by Alex on 2018/9/10.
 */
@Slf4j
public class ElasticInsert extends BaseElasticAction {

  public ElasticInsert(String indexName, String typeName, int offset, int insertSize,
      int batchSize) {
    super(indexName, typeName, offset, insertSize, batchSize);
  }

  @Override
  public void run() {
    StopWatch stopWatch = new StopWatch();

    int avgTime = 0;
    for (int i = maxOffset; i < maxOffset + insertSize; i++) {
      List<String> jsonData = DataFactory.generateDocumentList(batchSize);
      stopWatch.start();

      ElasticUtils.bulkInsert(indexName, typeName, jsonData, i);

      stopWatch.stop();

      // 计算平均插入时间
      avgTime += stopWatch.totalTime().getMillis();
      if (i % 50 == 0 && i != 0) {
        log.info("Avg insert " + batchSize + " time: " + avgTime / 50.0 / 1000.0 + "s.");
        avgTime = 0;
      }

      stopWatch = new StopWatch();

    }
  }
}
