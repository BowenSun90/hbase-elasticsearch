package com.alex.space.hbase;

import com.alex.space.common.CommonConstants;
import com.alex.space.hbase.function.HBaseInsert;
import com.alex.space.hbase.function.HBaseSelect;
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
    options.addOption("table", "tableName", false, "table name, default: test_table");
    options.addOption("cf", "columnFamily", false, "column family, default: d");
    options.addOption("region", "regionNum", false, "region number, default: 10");
    options.addOption("offset", "maxOffset", false, "max offset, max is 99999999, default: 1");
    options.addOption("itn", "insertThreadNum", false, "insert thread number, default: 1");
    options.addOption("utn", "updateThreadNum", false, "update thread number, default: 1");
    options.addOption("stn", "selectThreadNum", false, "select thread number, default: 1");
    options.addOption("is", "insertSize", false, "insert record size, default: 100000");
    options.addOption("us", "updateSize", false, "update record size, default: 100000");
    options.addOption("ss", "selectSize", false, "select record size, default: 100000");
    options.addOption("batch", "batchSize", false, "batch record size, default: 1000");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    // 参数初始化
    String tableName = cmd.getOptionValue("table", "test_table");
    String cf = cmd.getOptionValue("cf", "d");
    int regionNum = Integer.parseInt(cmd.getOptionValue("region", "10"));
    int offset = Integer.parseInt(cmd.getOptionValue("offset", "1"));
    if (offset > CommonConstants.MAX_OFFSET) {
      log.error("Offset out of bounds, max offset is 99999999");
    }

    int insertThreadNum = Integer.parseInt(cmd.getOptionValue("itn", "1"));
    int updateThreadNum = Integer.parseInt(cmd.getOptionValue("utn", "0"));
    int selectThreadNum = Integer.parseInt(cmd.getOptionValue("stn", "0"));

    int insertSize = Integer.parseInt(cmd.getOptionValue("is", "10000"));
    int updateSize = Integer.parseInt(cmd.getOptionValue("us", "10000"));
    int selectSize = Integer.parseInt(cmd.getOptionValue("ss", "10000"));

    int batchSize = Integer.parseInt(cmd.getOptionValue("batch", "100"));

    log.info("Insert or update table: {}, column family: {}, region number: {} . \n"
            + "insertSize: {}, updateSize: {}, selectSize: {} . \n"
            + "insertThreadNum: {}, updateThreadNum: {}, selectThreadNum: {} . \n"
            + "batchSize: {}",
        tableName, cf, regionNum, insertSize, updateSize, selectSize,
        insertThreadNum, updateThreadNum, selectThreadNum, batchSize);

    // 初始化测试表，如果不存在创建
    // Rowkey预分区，根据RegionNum，将MAX_ID平均分成RegionNum段
    byte[][] regions = new byte[regionNum][1];
    for (int i = 1; i < regionNum; i++) {
      int split = CommonConstants.MAX_OFFSET / regionNum * i;
      String splitKey = String.format("%08d", split);
      regions[i] = Bytes.toBytes(splitKey);
    }

    HBaseUtils hBaseUtils = HBaseUtils.getInstance();
    hBaseUtils.createTable(tableName, cf, regions);

    // Start test code
    ExecutorService insertPool = null, updatePool = null, selectPool = null;
    if (insertThreadNum > 0) {
      insertPool = Executors.newFixedThreadPool(insertThreadNum);
      insertPool.submit(new HBaseInsert(tableName, cf, offset, insertSize, batchSize));
      insertPool.shutdown();
    }

    if (updateThreadNum > 0) {
      updatePool = Executors.newFixedThreadPool(updateThreadNum);
      updatePool.submit(new HBaseUpdate(tableName, cf, offset, insertSize, batchSize));
      updatePool.shutdown();
    }

    if (selectThreadNum > 0) {
      selectPool = Executors.newFixedThreadPool(selectThreadNum);
      selectPool.submit(new HBaseSelect(offset));
      selectPool.shutdown();
    }

    while ((insertPool != null && !insertPool.isTerminated()) ||
        (updatePool != null && !updatePool.isTerminated()) ||
        (selectPool != null && !selectPool.isTerminated())) {
      Thread.sleep(10000);
    }

    System.exit(0);

  }

}
