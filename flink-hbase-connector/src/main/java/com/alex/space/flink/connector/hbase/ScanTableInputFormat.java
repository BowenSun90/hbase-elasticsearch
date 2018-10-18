package com.alex.space.flink.connector.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * ScanTableInputFormat
 *
 * ClientSideRegionScan
 *
 * @author Alex Created by Alex on 2018/9/19.
 */
@Slf4j
public abstract class ScanTableInputFormat<T> extends RichInputFormat<T, ScanTableInputSplit> {

  private String tableNameStr;
  private String cf;
  private List<String> qualifiers;
  private transient Table table;
  private transient HTable hTable;
  private transient FileSystem fs;
  private transient Path root;
  private transient org.apache.hadoop.conf.Configuration conf;
  private transient Configuration parameters;

  private ClientSideRegionScanner resultScanner;
  private transient HRegionLocation hRegionLocation;
  private transient Scan scan = null;

  private int batchSize;
  private byte[] currentRow;
  private long scannedRows;

  private boolean endReached = false;

  private ScanTableInputSplit split;

  /**
   * HBase returns an instance of {@link Result}.
   *
   * <p>This method maps the returned {@link Result} instance into the output type T
   *
   * @param r The Result instance from HBase that needs to be converted
   * @return The appropriate instance of T that contains the data of Result.
   */
  public abstract T mapResultToOutType(Result r);

  /**
   * Constructor
   *
   * @param tableNameStr hbase table name
   * @param cf hbase column family
   * @param qualifiers hbase qualifier list
   */
  public ScanTableInputFormat(String tableNameStr, String cf, List<String> qualifiers) {
    this.tableNameStr = tableNameStr;
    this.cf = cf;
    this.qualifiers = qualifiers;

    log.info("ClientScanTableInputFormat constructor.");
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
      this.parameters = parameters;

      this.conf = HBaseConfiguration.create();
      this.conf.set("hbase.zookeeper.quorum",
          parameters.getString(HBaseConfigConstants.HBASE_ZK_QUORUM, ""));
      this.conf.set("hbase.zookeeper.property.clientPort",
          parameters.getString(HBaseConfigConstants.HBASE_ZK_PORT, ""));
      this.conf.set("hbase.rootdir",
          parameters.getString(HBaseConfigConstants.HBASE_ROOT_DIR, ""));
      this.conf.set("fs.defaultFS",
          parameters.getString(HBaseConfigConstants.DEFAULT_FS, ""));
      this.conf.set("hbase.scanner.timeout.period",
          parameters.getString(HBaseConfigConstants.HBASE_SCAN_TIMEOUT, "60000"));

      this.conf.set("zookeeper.session.timeout", "180000");
      this.conf.set("hbase.rpc.timeout", "50000");
      this.conf.set("hbase.client.retries.number", "2");
      this.conf.set("zookeeper.recovery.retry", "3");
      this.conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

      this.batchSize = this.parameters.getInteger(HBaseConfigConstants.HBASE_SCAN_BATCH, 1000);

      Connection connection = ConnectionFactory.createConnection(conf);
      TableName tableName = TableName.valueOf(tableNameStr);
      this.table = connection.getTable(tableName);
      this.hTable = new HTable(conf, tableName);
      this.fs = FileSystem.get(conf);
      this.root = FSUtils.getRootDir(conf);

      this.scan = getScan();

    } catch (Exception e) {
      log.error("ClientScanTableInputFormat configure with exception: {}.", e.getMessage());
    }

    log.info("ClientScanTableInputFormat configure.");
  }

  private Scan getScan() {
    Scan scan = new Scan();
    scan.setBatch(this.batchSize);
    scan.addFamily(this.cf.getBytes());
    scan.setCacheBlocks(false);

    if (this.qualifiers != null && this.qualifiers.size() > 0) {
      List<Filter> filters = this.qualifiers.stream()
          .map(q -> new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(q))))
          .collect(Collectors.toList());

      Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
      scan.setFilter(filterList);
    }

    return scan;
  }

  @Override
  public void open(ScanTableInputSplit split) throws IOException {
    logSplitInfo(split);
    this.split = split;

    try {
      final List<HRegionLocation> locations = this.hTable
          .getRegionLocator().getAllRegionLocations();
      for (HRegionLocation location : locations) {
        if (location.getRegionInfo().getRegionNameAsString()
            .equalsIgnoreCase(split.getRegionName())) {
          this.hRegionLocation = location;
          break;
        }
      }

      if (this.hRegionLocation != null) {
        if (split.getStartRow() != null && split.getStartRow().length == 0) {
          this.scan.setStartRow(split.getStartRow());
          this.scan.setStopRow(split.getEndRow());
        }
        this.resultScanner = getClientScanner();
        this.endReached = false;
        this.scannedRows = 0;

        log.info("Open region: {}.", this.hRegionLocation.getRegionInfo().getRegionNameAsString());

      } else {
        log.error("Open nothing with region name: {}.", split.getRegionName());
      }

    } catch (Exception e) {
      log.error("Open region with exception, error: {}, region name: {}.",
          e.getMessage(), split.getRegionName());
    }

  }

  private void logSplitInfo(ScanTableInputSplit split) {
    int splitId = split.getSplitNumber();
    String[] hostNames = split.getHostnames();
    String regionName = split.getRegionName();
    String tableName = Bytes.toString(split.getTableName());
    String splitStart = Bytes.toString(split.getStartRow());
    String splitEnd = Bytes.toString(split.getEndRow());
    String splitStartKey = splitStart == null || splitStart.isEmpty() ? "-" : splitStart;
    String splitStopKey = splitStart == null || splitStart.isEmpty() ? "-" : splitEnd;

    log.info("Opening split (this={})[{}|{}|{}|{}|{}|{}]",
        this, splitId, hostNames, tableName, regionName, splitStartKey, splitStopKey);

  }

  private ClientSideRegionScanner getClientScanner() throws IOException {

    log.info(
        "Create ClientSideRegionScanner \n conf: {} \n fs: {} \n rootDir: {} \n htd: {} \n hri: {} \n scan: {}",
        this.conf, this.fs, this.root, this.table, this.hRegionLocation, this.scan);

    return new ClientSideRegionScanner(
        this.conf,
        this.fs,
        this.root,
        new ReadOnlyTableDescriptor(this.table.getTableDescriptor()),
        this.hRegionLocation.getRegionInfo(),
        this.scan,
        null);
  }


  @Override
  public T nextRecord(T reuse) throws IOException {
    if (this.resultScanner == null) {
      throw new IOException("No table result scanner provided!");
    }

    try {
      Result res = this.resultScanner.next();
      if (res != null) {
        this.scannedRows++;
        this.currentRow = res.getRow();
        return mapResultToOutType(res);
      }
    } catch (Exception e) {
      this.resultScanner.close();
      log.warn("Error after scan of " + this.scannedRows + " rows, currentRow: "
          + new String(this.currentRow) + ".", e);
      this.scan.setStartRow(currentRow);
      this.resultScanner = getClientScanner();
      Result res = this.resultScanner.next();
      if (res != null) {
        this.scannedRows++;
        this.currentRow = res.getRow();
        return mapResultToOutType(res);
      }
    }

    this.endReached = true;
    return null;
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return endReached;
  }

  @Override
  public void close() throws IOException {
    log.info("Closing split {}[{}] (scanned {} rows)",
        split.getRegionName(),
        split.getStartRow(),
        scannedRows);
    currentRow = null;
  }

  @Override
  public void closeInputFormat() throws IOException {
    log.info("Closing connection, table {}.", table.getName());
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
    Map<String, String> map = new HashMap<>(128);

    for (HRegionLocation location : hRegionLocations) {
      HRegionInfo hRegionInfo = location.getRegionInfo();
      log.info("Region: {}, id: {}, name: {}, location: {}.",
          hRegionInfo,
          hRegionInfo.getRegionId(),
          hRegionInfo.getRegionNameAsString(),
          location);
      map.put(hRegionInfo.getRegionNameAsString(), location.getHostnamePort());

      byte[] startKey = hRegionInfo.getStartKey();
      byte[] endKey = hRegionInfo.getEndKey();
      byte[] splitKey = getSplitKey(startKey, endKey, true);
      log.info("Start:{}, End:{}, Mid:{}, Host:{}",
          Bytes.toString(startKey), Bytes.toString(endKey), Bytes.toString(splitKey),
          location.getHostnamePort());
    }

    for (Map.Entry<String, String> entry : map.entrySet()) {
      final String[] hosts = new String[]{entry.getValue()};
      splits.add(
          new ScanTableInputSplit(splits.size(), hosts, table.getName().getName(),
              entry.getKey(), null, null));
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

  private byte[] getSplitKey(byte[] start, byte[] end, boolean isText) {
    byte upperLimitByte;
    byte lowerLimitByte;
    //Use text mode or binary mode.
    if (isText) {
      //The range of text char set in ASCII is [32,126], the lower limit is space and the upper
      // limit is '~'.
      upperLimitByte = '~';
      lowerLimitByte = ' ';
    } else {
      upperLimitByte = Byte.MAX_VALUE;
      lowerLimitByte = Byte.MIN_VALUE;
    }
    // For special case
    // Example 1 : startkey=null, endkey="hhhqqqwww", splitKey="h"
    // Example 2 (text key mode): startKey="ffffaaa", endKey=null, splitkey="f~~~~~~"
    if (start.length == 0 && end.length == 0) {
      return new byte[]{(byte) ((lowerLimitByte + upperLimitByte) / 2)};
    }
    if (start.length == 0 && end.length != 0) {
      return new byte[]{end[0]};
    }
    if (start.length != 0 && end.length == 0) {
      byte[] result = new byte[start.length];
      result[0] = start[0];
      for (int k = 1; k < start.length; k++) {
        result[k] = upperLimitByte;
      }
      return result;
    }
    // A list to store bytes in split key
    List resultBytesList = new ArrayList();
    int maxLength = start.length > end.length ? start.length : end.length;
    for (int i = 0; i < maxLength; i++) {
      //calculate the midpoint byte between the first difference
      //for example: "11ae" and "11chw", the midpoint is "11b"
      //another example: "11ae" and "11bhw", the first different byte is 'a' and 'b',
      // there is no midpoint between 'a' and 'b', so we need to check the next byte.
      if (start[i] == end[i]) {
        resultBytesList.add(start[i]);
        //For special case like: startKey="aaa", endKey="aaaz", splitKey="aaaM"
        if (i + 1 == start.length) {
          resultBytesList.add((byte) ((lowerLimitByte + end[i + 1]) / 2));
          break;
        }
      } else {
        //if the two bytes differ by 1, like ['a','b'], We need to check the next byte to find
        // the midpoint.
        if ((int) end[i] - (int) start[i] == 1) {
          //get next byte after the first difference
          byte startNextByte = (i + 1 < start.length) ? start[i + 1] : lowerLimitByte;
          byte endNextByte = (i + 1 < end.length) ? end[i + 1] : lowerLimitByte;
          int byteRange = (upperLimitByte - startNextByte) + (endNextByte - lowerLimitByte) + 1;
          int halfRange = byteRange / 2;
          if ((int) startNextByte + halfRange > (int) upperLimitByte) {
            resultBytesList.add(end[i]);
            resultBytesList.add((byte) (startNextByte + halfRange - upperLimitByte +
                lowerLimitByte));
          } else {
            resultBytesList.add(start[i]);
            resultBytesList.add((byte) (startNextByte + halfRange));
          }
        } else {
          //calculate the midpoint key by the fist different byte (normal case),
          // like "11ae" and "11chw", the midpoint is "11b"
          resultBytesList.add((byte) ((start[i] + end[i]) / 2));
        }
        break;
      }
    }
    //transform the List of bytes to byte[]
    byte result[] = new byte[resultBytesList.size()];
    for (int k = 0; k < resultBytesList.size(); k++) {
      result[k] = (byte) resultBytesList.get(k);
    }
    return result;
  }

}
