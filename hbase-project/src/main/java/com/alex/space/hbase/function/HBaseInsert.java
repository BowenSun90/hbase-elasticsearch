package com.alex.space.hbase.function;

import com.alex.space.hbase.model.BatchData;
import com.alex.space.hbase.utils.DataFactory;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;

/**
 * Insert into Hbase
 *
 * @author Alex Created by Alex on 2018/9/8.
 */
@Slf4j
public class HBaseInsert extends BaseHBaseAction {

  public HBaseInsert(String tableName, String cf, int offset, int insertSize, int batchSize,
      int maxColNum, int minColNum) {
    super(tableName, cf, offset, insertSize, batchSize, maxColNum, minColNum);
  }

  /**
   * 按id递增插入Hbase
   *
   * 随机更新字段
   */
  @Override
  public void run() {

    try {
      StopWatch stopWatch = new StopWatch();
      int avgTime = 0;

      int optCount = 0;
      List<BatchData> batchDataList = new ArrayList<>(batchSize);
      for (int i = maxOffset; i < maxOffset + insertSize; i++) {
        BatchData batchData = generateBatchData(i);
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
          if (optCount % 20 == 0 && optCount != 0) {
            log.info("Avg insert " + batchSize + " time: " + avgTime / 20.0 / 1000.0 + "s.");
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

  /**
   * Create hbase content
   *
   * @param offset offset
   */
  protected BatchData generateBatchData(int offset) {
    return DataFactory.generateBatchData(offset, minColNum, maxColNum);
  }

}
