package com.alex.space.hbase;

import com.alex.space.common.CommonConstants;
import com.alex.space.hbase.function.HBaseInsert;
import com.alex.space.hbase.function.HBaseSelect;
import com.alex.space.hbase.function.HBaseStatistics;
import com.alex.space.hbase.function.HBaseUpdate;
import com.alex.space.hbase.utils.HBaseUtils;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase数据插入
 *
 * @author Alex Created by Alex on 2018/9/8.
 */
@Slf4j
public class HBaseInit {

  public static void main(String[] args) throws IOException, ParseException, InterruptedException {
    Options options = new Options();
    options.addOption("table", "tableName", true, "table name, default: test_table");
    options.addOption("cf", "columnFamily", true, "column family, default: d");
    options.addOption("region", "regionNum", true, "region number, default: 10");
    options.addOption("offset", "maxOffset", true, "max offset, max is 99999999, default: 1");
    options.addOption("itn", "insertThreadNum", true, "insert thread number, default: 1");
    options.addOption("utn", "updateThreadNum", true, "update thread number, default: 1");
    options.addOption("stn", "selectThreadNum", true, "select thread number, default: 1");
    options.addOption("is", "insertSize", true, "insert record size, default: 100000");
    options.addOption("us", "updateSize", true, "update record size, default: 100000");
    options.addOption("ss", "selectSize", true, "select record size, default: 100000");
    options.addOption("batch", "batchSize", true, "batch record size, default: 1000");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    // 参数初始化
    String tableName = cmd.getOptionValue("table", "c0_trait");
    String cf = cmd.getOptionValue("cf", "d");
    int regionNum = Integer.parseInt(cmd.getOptionValue("region", "100"));
    int offset = Integer.parseInt(cmd.getOptionValue("offset", "1"));
    if (offset > CommonConstants.MAX_OFFSET) {
      log.error("Offset out of bounds, max offset is 99999999");
    }

    int insertThreadNum = Integer.parseInt(cmd.getOptionValue("itn", "0"));
    int updateThreadNum = Integer.parseInt(cmd.getOptionValue("utn", "0"));
    int selectThreadNum = Integer.parseInt(cmd.getOptionValue("stn", "0"));

    int insertSize = Integer.parseInt(cmd.getOptionValue("is", "100000"));
    int updateSize = Integer.parseInt(cmd.getOptionValue("us", "100000"));
    int selectSize = Integer.parseInt(cmd.getOptionValue("ss", "100000"));

    int batchSize = Integer.parseInt(cmd.getOptionValue("batch", "1000"));

    log.info("Insert or update table: {}, column family: {}, region number: {} . \n"
            + "insertSize: {}, updateSize: {}, selectSize: {} . \n"
            + "insertThreadNum: {}, updateThreadNum: {}, selectThreadNum: {} . \n"
            + "batchSize: {}, offset: {}",
        tableName, cf, regionNum, insertSize, updateSize, selectSize,
        insertThreadNum, updateThreadNum, selectThreadNum, batchSize, offset);

    // 初始化测试表，如果不存在创建
    // Rowkey预分区，根据RegionNum，将MAX_ID平均分成RegionNum段
    byte[][] regions = new byte[regionNum][1];
    for (int i = 1; i < regionNum; i++) {
      int split = CommonConstants.MAX_OFFSET / regionNum * i;
      String splitKey = String.format(CommonConstants.ROWKEY_FORMAT, split);
      regions[i] = Bytes.toBytes(splitKey);
    }

    HBaseUtils hBaseUtils = HBaseUtils.getInstance();
    hBaseUtils.createTable(tableName, cf, regions);

    // Start test code
    ExecutorService insertPool = null, updatePool = null, selectPool = null;
    if (insertThreadNum > 0 && insertSize > 0) {
      insertPool = Executors.newFixedThreadPool(insertThreadNum);
      for (int i = 0; i < insertThreadNum; i++) {
        int subInsertSize = insertSize / insertThreadNum;
        int startOffset = offset + subInsertSize * i;
        insertPool.submit(new HBaseInsert(tableName, cf, startOffset, subInsertSize, batchSize));
      }
      insertPool.shutdown();
    }

    if (updateThreadNum > 0 && updateSize > 0) {
      updatePool = Executors.newFixedThreadPool(updateThreadNum);
      for (int i = 0; i < updateThreadNum; i++) {
        int subUpdateSize = updateSize / updateThreadNum;
        updatePool.submit(new HBaseUpdate(tableName, cf, offset, subUpdateSize, batchSize));
      }
      updatePool.shutdown();
    }

    if (selectThreadNum > 0 && selectSize > 0) {
      selectPool = Executors.newFixedThreadPool(selectThreadNum);
      for (int i = 0; i < selectThreadNum; i++) {
        int subSelectSize = selectSize / updateThreadNum;
        selectPool.submit(new HBaseSelect(tableName, cf, offset, subSelectSize, batchSize));
      }
      selectPool.shutdown();
    }

    while ((insertPool != null && !insertPool.isTerminated()) ||
        (updatePool != null && !updatePool.isTerminated()) ||
        (selectPool != null && !selectPool.isTerminated())) {
      Thread.sleep(10000);
    }

    HBaseStatistics statistics = new HBaseStatistics(tableName, cf);
    statistics.printStatistics1();

    System.exit(0);

  }

}
