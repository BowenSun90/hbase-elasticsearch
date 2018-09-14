package com.alex.space.elastic.function;

import com.alex.space.elastic.utils.DataFactory;
import com.alex.space.elastic.utils.ElasticUtils;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.StopWatch;

/**
 * ElasticSearch update code
 *
 * @author Alex Created by Alex on 2018/9/10.
 */
@Slf4j
public class ElasticUpdate extends BaseElasticAction {

  public ElasticUpdate(String indexName, String typeName, int offset, int insertSize,
      int batchSize) {
    super(indexName, typeName, offset, insertSize, batchSize);
  }

  @Override
  public void run() {

    StopWatch stopWatch = new StopWatch();

    int avgTime = 0;
    for (int i = 0; i < maxOffset; i++) {
      List<String> jsonData = DataFactory.generateDocumentList(batchSize);
      stopWatch.start();

      ElasticUtils.bulkUpdate(indexName, typeName, jsonData,
          ThreadLocalRandom.current().nextInt(i));

      stopWatch.stop();

      // 计算平均插入时间
      avgTime += stopWatch.totalTime().getMillis();
      if (i % 20 == 0 && i != 0) {
        log.info("Avg update " + batchSize + " time: " + avgTime / 20.0 / 1000.0 + "s.");
        avgTime = 0;
      }
      stopWatch = new StopWatch();

    }
  }
}
