package com.alex.space.hbase.hfile;

import com.alex.space.hbase.config.HBaseConfig;
import com.alex.space.hbase.config.HBaseConstants;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * @author Alex Created by Alex on 2018/9/19.
 */
@Slf4j
public class ClientSideScanner {

  private static HBaseConfig hBaseConfig = HBaseConfig.getInstance();

  private Configuration conf;

  private Connection connection;

  private ExecutorService pool = Executors.newScheduledThreadPool(10);

  private Scan scan;

  private int rows = 0;
  private int sampleRows = 0;
  private int sampleCols = 0;

  public ClientSideScanner() {
    try {
      conf = HBaseConfiguration.create();

      conf.set("hbase.zookeeper.quorum", hBaseConfig.getProperty(HBaseConstants.ZOOKEEPER_QUORUM));
      conf.set("hbase.zookeeper.property.clientPort",
          hBaseConfig.getProperty(HBaseConstants.ZOOKEEPER_PORT));

      conf.set("hbase.rootdir", hBaseConfig.getProperty(HBaseConstants.ROOT_DIR));
      conf.set("fs.defaultFS", hBaseConfig.getProperty(HBaseConstants.DEFAULT_FS));

      conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

      connection = ConnectionFactory.createConnection(conf, pool);

      scan = new Scan();
      scan.setBatch(100);
      scan.setCacheBlocks(false);


    } catch (Exception e) {
      log.error(e.getMessage());
    }
  }

  public void tableScan(String table, String cf) throws Exception {
    scan.addFamily(Bytes.toBytes(cf));

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    TableName tableName = TableName.valueOf(table);
    Table hTable = connection.getTable(tableName);
    HTableDescriptor htd = hTable.getTableDescriptor();

    Admin hAdmin = connection.getAdmin();
    List<HRegionInfo> hRegionInfoList = hAdmin.getTableRegions(tableName);

    FileSystem fs = FileSystem.get(conf);
    Path root = FSUtils.getRootDir(conf);

    for (HRegionInfo hRegionInfo : hRegionInfoList) {
      regionScan(fs, root, htd, hRegionInfo);
    }

    stopWatch.stop();
    log.info("Total rows: {}, avg columns: {}, time: {}s",
        rows, sampleCols / sampleRows, stopWatch.getTime() / 1000.0);
  }

  public void regionScan(FileSystem fs, Path root, HTableDescriptor htd, HRegionInfo hRegionInfo)
      throws Exception {

    log.info("Scan region: {}", hRegionInfo.getRegionNameAsString());
    ClientSideRegionScanner scanner =
        new ClientSideRegionScanner(
            conf,
            fs,
            root,
            new ReadOnlyTableDescriptor(htd),
            hRegionInfo,
            scan,
            null);

    Result result;
    while ((result = scanner.next()) != null) {
      if (rows % 100 == 0) {
        sampleRows++;
        sampleCols += result.rawCells().length;
      }
      rows++;
      printScanResult(result);
    }

    scanner.close();

  }

  private void printScanResult(Result result) {
    String rowkey = Bytes.toString(result.getRow());
    for (Cell cell : result.rawCells()) {
      printCell(rowkey, cell);
    }
  }

  private void printCell(String rowKey, Cell cell) {
    String cf = Bytes.toString(CellUtil.cloneFamily(cell));
    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
    String value = Bytes.toString(CellUtil.cloneValue(cell));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    log.debug("Row: {}, {}:{} {}, {}", rowKey, cf, qualifier, value,
        sdf.format(new Date(cell.getTimestamp())));

  }

  public static void main(String[] args) {
    ClientSideScanner clientSideScanner = new ClientSideScanner();
    try {
      clientSideScanner.tableScan("test_table", "d");
    } catch (Exception e) {
      e.getMessage();
    }
  }
}
