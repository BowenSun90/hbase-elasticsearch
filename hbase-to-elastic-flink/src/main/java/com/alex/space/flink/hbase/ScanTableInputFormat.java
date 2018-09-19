package com.alex.space.flink.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alex Created by Alex on 2018/9/19.
 */
public class ScanTableInputFormat extends RichInputFormat<RowData, ScanTableInputSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(ScanTableInputFormat.class);

  private String tableNameStr;
  private transient TableName tableName;
  private transient Table table;
  private transient HTable hTable;
  private transient FileSystem fs;
  private transient Path root;
  private transient org.apache.hadoop.conf.Configuration conf;

  private ClientSideRegionScanner scanner;

  private long scannedRows;
  private byte[] currentRow;

  private boolean endReached = false;

  public ScanTableInputFormat(String tableNameStr) {
    this.tableNameStr = tableNameStr;

    LOG.info("ScanTableInputFormat constructor");
  }

  /**
   * HBase returns an instance of {@link Result}.
   *
   * <p>This method maps the returned {@link Result} instance into the output type {@link RowData}.
   *
   * @param r The Result instance from HBase that needs to be converted
   * @return The appropriate instance of {@link RowData} that contains the data of Result.
   */
  private RowData mapResultToOutType(Result r) {
    RowData rowData = new RowData();
    rowData.setRow(Bytes.toString(r.getRow()));
    Map<String, String> map = new HashMap<>(64);
    for (Cell cell : r.rawCells()) {
      map.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
          Bytes.toString(CellUtil.cloneValue(cell)));
    }
    rowData.setKeyValue(map);
    rowData.setCells(r.rawCells());
    return rowData;
  }

  /**
   * Creates a {@link Scan} object and opens the {@link HTable} connection.
   *
   * <p>These are opened here because they are needed in the createInputSplits which is called
   * before the openInputFormat method.
   *
   * <p>The connection is opened in this method and closed in {@link #closeInputFormat()}.
   *
   * @param parameters The configuration that is to be used
   * @see Configuration
   */
  @Override
  public void configure(Configuration parameters) {
    try {
      conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.quorum", "172.23.4.138,172.23.4.139,172.23.4.140");
      conf.set("hbase.zookeeper.property.clientPort", "2181");
      conf.set("hbase.rootdir", "hdfs://namenode01.td.com/hbase");
      conf.set("fs.defaultFS", "hdfs://namenode01.td.com");

      ExecutorService pool = Executors.newScheduledThreadPool(10);
      Connection connection = ConnectionFactory.createConnection(conf, pool);

      tableName = TableName.valueOf(tableNameStr);
      table = connection.getTable(tableName);
      hTable = new HTable(conf, tableName);
      fs = FileSystem.get(conf);
      root = FSUtils.getRootDir(conf);

    } catch (Exception e) {
      e.printStackTrace();
    }

    LOG.info("ScanTableInputFormat configure");
  }

  @Override
  public void open(ScanTableInputSplit split) throws IOException {
    logSplitInfo("opening", split);

    try {

      List<HRegionLocation> locations = hTable.getRegionLocator().getAllRegionLocations();
      HRegionLocation regionLocation = null;
      for (HRegionLocation location : locations) {
        if (location.getRegionInfo().getRegionNameAsString()
            .equalsIgnoreCase(split.getRegionName())) {
          regionLocation = location;
          break;
        }
      }

      if (regionLocation != null) {

        Scan scan = new Scan();
        scan.setBatch(100);
        scan.setCacheBlocks(false);
        scan.addFamily("d".getBytes());

        HRegionInfo regionInfo = regionLocation.getRegionInfo();

        scanner = new ClientSideRegionScanner(
            conf,
            fs,
            root,
            new ReadOnlyTableDescriptor(table.getTableDescriptor()),
            regionInfo,
            scan,
            null);

      } else {
        LOG.error("open nothing");
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public RowData nextRecord(RowData reuse) throws IOException {
    if (scanner == null) {
      throw new IOException("No table result scanner provided!");
    }

    try {
      Result res = scanner.next();
      if (res != null) {
        scannedRows++;
        currentRow = res.getRow();
        return mapResultToOutType(res);
      }
    } catch (Exception e) {
      scanner.close();
      LOG.warn("Error after scan of " + scannedRows + " rows. ", e);
      // TODO Retry
      e.printStackTrace();
    }

    endReached = true;
    return null;
  }

  private void logSplitInfo(String action, ScanTableInputSplit split) {
    int splitId = split.getSplitNumber();
    String[] hostNames = split.getHostnames();
    String regionName = split.getRegionName();
    LOG.info("{} split (this={})[{}|{}|{}]", action, this, splitId, hostNames, regionName);
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return endReached;
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing split (scanned rows)");

    try {
      if (fs != null) {
        fs.close();
      }

    } finally {
      fs = null;
    }
  }

  @Override
  public void closeInputFormat() throws IOException {
    try {
      if (table != null) {
        table.close();
      }
      if (hTable != null) {
        hTable.close();
      }
    } finally {
      table = null;
      hTable = null;
    }
  }

  @Override
  public ScanTableInputSplit[] createInputSplits(final int minNumSplits) throws IOException {
    final List<ScanTableInputSplit> splits = new ArrayList<>(minNumSplits);
    List<HRegionLocation> hRegionLocations = hTable.getRegionLocator().getAllRegionLocations();
    Map<String, String> map = new HashMap<>();
    for (HRegionLocation location : hRegionLocations) {
      HRegionInfo hRegionInfo = location.getRegionInfo();
      LOG.info("Region: {}, id: {}, name: {}", hRegionInfo, hRegionInfo.getRegionId(),
          hRegionInfo.getRegionNameAsString());
      map.put(hRegionInfo.getRegionNameAsString(), location.getHostnamePort());
    }

    for (Map.Entry<String, String> entry : map.entrySet()) {
      final String[] hosts = new String[]{entry.getValue()};
      splits.add(
          new ScanTableInputSplit(splits.size(), hosts, table.getName().getName(),
              entry.getKey()));
    }
    return splits.toArray(new ScanTableInputSplit[splits.size()]);
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(ScanTableInputSplit[] inputSplits) {
    return new LocatableInputSplitAssigner(inputSplits);
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
    return null;
  }

}
