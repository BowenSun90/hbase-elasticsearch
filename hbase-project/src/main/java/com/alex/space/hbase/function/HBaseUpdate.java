package com.alex.space.hbase.function;

import com.alex.space.hbase.model.BatchData;
import com.alex.space.hbase.utils.DataFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;

/**
 * Update on Hbase
 *
 * @author Alex Created by Alex on 2018/9/8.
 */
@Slf4j
public class HBaseUpdate extends BaseHBaseAction {

  public HBaseUpdate(String tableName, String cf, int offset, int insertSize, int batchSize) {
    super(tableName, cf, offset, insertSize, batchSize);
  }

  /**
   * 随机ID更新Hbase
   *
   * 随机更新字段
   */
  @Override
  public void run() {

    try {
      StopWatch stopWatch = new StopWatch();
      int avgTime = 0;

      List<BatchData> batchDataList = new ArrayList<>(batchSize);

      int optCount = 0;
      for (int i = 0; i < insertSize; i++) {
        int randomId = ThreadLocalRandom.current().nextInt(maxOffset);
        BatchData batchData = DataFactory.generateBatchData(randomId);
        batchDataList.add(batchData);

        // 批量插入
        if (batchDataList.size() == batchSize) {
          optCount++;
          stopWatch.start();

          hBaseUtils.batchPut(tableName, cf, batchDataList);
          batchDataList.clear();

          stopWatch.stop();

          // 计算平均时常
          avgTime += stopWatch.getTime();
          if (optCount % 50 == 0 && optCount != 0) {
            log.info("Avg update " + batchSize + " time: " + avgTime / 50.0 / 1000.0 + "s.");
            avgTime = 0;
          }
          stopWatch = new StopWatch();
        }

        log.debug("Put: " + i + "[" + batchData.getRowKey() + "]");
      }

      if (batchDataList.size() != 0) {
        hBaseUtils.batchPut(tableName, cf, batchDataList);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
