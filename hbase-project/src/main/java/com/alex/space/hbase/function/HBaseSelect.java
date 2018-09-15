package com.alex.space.hbase.function;

import com.alex.space.hbase.model.BatchData;
import com.alex.space.hbase.utils.DataFactory;
import com.alex.space.hbase.utils.HBaseUtils;
import com.alex.space.hbase.utils.RowKeyUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;

/**
 * Select from Hbase
 *
 * @author Alex Created by Alex on 2018/9/8.
 */
@Slf4j
public class HBaseSelect extends BaseHBaseAction {

  private HBaseUtils hBaseUtils = HBaseUtils.getInstance();

  public HBaseSelect(String tableName, String cf, int offset, int insertSize, int batchSize) {
    super(tableName, cf, offset, insertSize, batchSize);
  }

  /**
   * 随机ID查询Hbase
   *
   * 随机查询字段
   */
  @Override
  public void run() {
    StopWatch stopWatch = new StopWatch();
    int avgTime = 0;

    List<BatchData> batchDataList = new ArrayList<>(batchSize);
    int optCount = 0;

    for (int i = 0; i < insertSize; i++) {
      int randomId = ThreadLocalRandom.current().nextInt(maxOffset);
      BatchData batchData = DataFactory.generateSelectData(randomId);
      batchDataList.add(batchData);

      if (batchDataList.size() == batchSize) {
        optCount++;
        stopWatch.start();
        hBaseUtils.batchGet(tableName, cf, batchDataList);

        batchDataList.clear();
        stopWatch.stop();

        // 计算平均时常
        avgTime += stopWatch.getTime();
        if (optCount % 20 == 0 && optCount != 0) {
          log.info("Avg select " + batchSize + " time: " + avgTime / 20.0 / 1000.0 + "s.");
          avgTime = 0;
        }
        stopWatch = new StopWatch();
      }

      log.debug("Get: " + i + "[" + RowKeyUtils.buildNumberRowkey(randomId) + "]");
    }

  }

}
