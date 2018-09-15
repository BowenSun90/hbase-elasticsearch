package com.alex.space.elastic;

import com.alex.space.common.CommonConstants;
import com.alex.space.elastic.function.ElasticInsert;
import com.alex.space.elastic.function.ElasticQuery;
import com.alex.space.elastic.function.ElasticUpdate;
import com.alex.space.elastic.utils.ElasticUtils;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * 插入测试数据
 *
 * @author Alex Created by Alex on 2018/9/4.
 */
@Slf4j
public class ElasticInit {

  public static void main(String[] args)
      throws IOException, ParseException, InterruptedException {

    Options options = new Options();
    options.addOption("index", "indexName", false, "index name, default: test_table");
    options.addOption("type", "typeName", false, "type name, default: d");
    options.addOption("offset", "maxOffset", false, "max offset, max is 99999999, default: 1");
    options.addOption("itn", "insertThreadNum", false, "insert thread number, default: 1");
    options.addOption("utn", "updateThreadNum", false, "update thread number, default: 1");
    options.addOption("stn", "selectThreadNum", false, "select thread number, default: 1");
    options.addOption("is", "insertSize", false, "insert record size, default: 100000");
    options.addOption("us", "updateSize", false, "update record size, default: 100000");
    options.addOption("ss", "selectSize", false, "select record size, default: 100000");
    options.addOption("batch", "batchSize", false, "batch record size, default: 1000");
    options.addOption("delete", "deleteExist", false, "delete exist index, default: false");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    // 参数初始化
    String indexName = cmd.getOptionValue("index", "test_table");
    String typeName = cmd.getOptionValue("type", "d");
    int offset = Integer.parseInt(cmd.getOptionValue("offset", "1"));
    if (offset > CommonConstants.MAX_OFFSET) {
      log.error("Offset out of bounds, max offset is 99999999");
    }

    int insertThreadNum = Integer.parseInt(cmd.getOptionValue("itn", "1"));
    int updateThreadNum = Integer.parseInt(cmd.getOptionValue("utn", "1"));
    int selectThreadNum = Integer.parseInt(cmd.getOptionValue("stn", "1"));

    int insertSize = Integer.parseInt(cmd.getOptionValue("is", "100000"));
    int updateSize = Integer.parseInt(cmd.getOptionValue("us", "100000"));
    int selectSize = Integer.parseInt(cmd.getOptionValue("ss", "100000"));

    int batchSize = Integer.parseInt(cmd.getOptionValue("batch", "1000"));
    boolean deleteExist = Boolean.parseBoolean(cmd.getOptionValue("delete", "false"));

    log.info("Insert or update index name: {}, type name: {} . \n"
            + "insertSize: {}, updateSize: {}, selectSize: {} . \n"
            + "insertThreadNum: {}, updateThreadNum: {}, selectThreadNum: {} . \n"
            + "batchSize: {}, deleteExist: {}",
        indexName, typeName, insertSize, updateSize, selectSize,
        insertThreadNum, updateThreadNum, selectThreadNum, batchSize, deleteExist);

    // Init elasticsearch connect
    ElasticUtils.init();
    ElasticUtils.createIndex(indexName, typeName, deleteExist);

    // Start test code
    ExecutorService insertPool = null, updatePool = null, selectPool = null;
    if (insertThreadNum > 0 && insertSize > 0) {
      insertPool = Executors.newFixedThreadPool(insertThreadNum);
      for (int i = 0; i < insertThreadNum; i++) {
        int subInsertSize = insertSize / insertThreadNum;
        int startOffset = offset + subInsertSize * i;
        insertPool.submit(
            new ElasticInsert(indexName, typeName, startOffset, subInsertSize, batchSize));
      }
      insertPool.shutdown();
    }

    if (updateThreadNum > 0 && updateSize > 0) {
      updatePool = Executors.newFixedThreadPool(updateThreadNum);
      for (int i = 0; i < updateThreadNum; i++) {
        int subUpdateSize = updateSize / updateThreadNum;
        updatePool.submit(new ElasticUpdate(indexName, typeName, offset, subUpdateSize, batchSize));
      }
      updatePool.shutdown();
    }

    if (selectThreadNum > 0 && selectSize > 0) {
      selectPool = Executors.newFixedThreadPool(selectThreadNum);
      for (int i = 0; i < selectThreadNum; i++) {
        int subSelectSize = selectSize / selectThreadNum;
        selectPool.submit(new ElasticQuery(indexName, typeName, offset, subSelectSize, batchSize));
      }
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
