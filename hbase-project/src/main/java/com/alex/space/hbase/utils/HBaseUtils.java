package com.alex.space.hbase.utils;

import com.alex.space.hbase.config.HBaseConfig;
import com.alex.space.hbase.config.HBaseConstants;
import com.alex.space.hbase.model.BatchData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase API
 *
 * @author Alex
 */
@Slf4j
public class HBaseUtils {

  private static HBaseConfig hBaseConfig = HBaseConfig.getInstance();

  /**
   * HBase conf
   */
  private static Configuration conf = HBaseConfiguration.create();

  /**
   * ScheduledThreadPool
   */
  private static ExecutorService pool = Executors.newScheduledThreadPool(10);

  /**
   * HBase connection
   */
  private static Connection connection = null;

  private static HBaseUtils instance = null;

  /**
   * Init HBase connection
   */
  private HBaseUtils() {
    if (connection == null) {
      try {
        conf.set(HBaseConstants.ZOOKEEPER_QUORUM,
            hBaseConfig.getProperty(HBaseConstants.ZOOKEEPER_QUORUM));
        conf.set(HBaseConstants.ZOOKEEPER_PORT,
            hBaseConfig.getProperty(HBaseConstants.ZOOKEEPER_PORT));

        connection = ConnectionFactory.createConnection(conf, pool);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static synchronized HBaseUtils getInstance() {
    if (instance == null) {
      instance = new HBaseUtils();
    }
    return instance;
  }

  /**
   * Create table
   *
   * @param tableName Table name
   * @param columnFamily Column family name
   * @param splitKeys Table split keys
   */
  public void createTable(String tableName, String columnFamily, byte[][] splitKeys)
      throws IOException {
    Admin admin = connection.getAdmin();
    TableName name = TableName.valueOf(tableName);

    if (admin.tableExists(name)) {
      log.warn("Table {} exists.", name);
    } else {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

      HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
      desc.addFamily(hColumnDescriptor);
      admin.createTable(desc, splitKeys);
      log.info("Create table {}.", tableName);
    }

    admin.close();
  }

  /**
   * Drop table
   *
   * @param tableName Table name
   */
  public void dropTable(String tableName) throws IOException {
    Admin admin = connection.getAdmin();
    TableName name = TableName.valueOf(tableName);
    if (admin.tableExists(name)) {
      log.warn("Table {} exists, disable and drop table.", name);
      admin.disableTable(name);
      admin.deleteTable(name);
    }

    admin.close();
  }

  /**
   * Put column value
   *
   * @param tableName Table name
   * @param row Row key
   * @param columnFamily Column family name
   * @param column Column name
   * @param value Value
   */
  public void put(String tableName, String row, String columnFamily, String column, String value) {
    try {
      TableName name = TableName.valueOf(tableName);
      Table table = connection.getTable(name);
      Put put = new Put(Bytes.toBytes(row));

      put.addColumn(
          Bytes.toBytes(columnFamily),
          Bytes.toBytes(String.valueOf(column)),
          Bytes.toBytes(value));

      table.put(put);

      table.close();
      log.info("Put table:{}, row:{}, columnFamily:{}, column:{}, value:{}", tableName, row,
          columnFamily, column, value);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void batchPut(String tableName, String cf, List<BatchData> batchDataList) {
    if (batchDataList == null || batchDataList.size() == 0) {
      log.error("batch put with error params.");
      return;
    }

    try {
      TableName name = TableName.valueOf(tableName);
      Table table = connection.getTable(name);
      List<Put> puts = new ArrayList<>();

      for (BatchData batchData : batchDataList) {
        if (StringUtils.isEmpty(batchData.getRowKey())
            || batchData.getColumns().length != batchData.getValues().length) {
          log.error("batch put with error params.");
          continue;
        }

        // create Put
        for (int i = 0; i < batchData.getColumns().length; i++) {
          Put put = new Put(Bytes.toBytes(batchData.getRowKey()));

          put.addImmutable(Bytes.toBytes(cf), Bytes.toBytes(batchData.getColumns()[i]),
              Bytes.toBytes(batchData.getValues()[i]));
          puts.add(put);
        }
      }

      // Put into table
      table.put(puts);
      table.close();
      log.info("Put table:{}, cf:{}, size:{} ", tableName, cf, batchDataList.size());

    } catch (Exception e) {
      log.error("batch put with exception: {}", e.getMessage());
    }
  }

  /**
   * Put set of column value
   *
   * @param tableName Table name
   * @param row Row key
   * @param columnFamily Column family name
   * @param columns Array of columns
   * @param values Array of values
   */
  public void put(String tableName, String row, String columnFamily, String[] columns,
      String[] values) {
    try {
      TableName name = TableName.valueOf(tableName);
      Table table = connection.getTable(name);

      List<Put> puts = new ArrayList<>(columns.length);

      for (int i = 0; i < columns.length; i++) {

        Put put = new Put(Bytes.toBytes(row));

        put.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]),
            Bytes.toBytes(values[i]));
        puts.add(put);
      }
      table.put(puts);

      table.close();
      log.info("Put table:{}, row:{}, columnFamily:{}, column:{}, value:{}", tableName, row,
          columnFamily, columns, values);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Select by row key
   *
   * @param tableName Table name
   * @param row Row key
   */
  public void selectRow(String tableName, String row) {
    try {
      TableName name = TableName.valueOf(tableName);
      Table table = connection.getTable(name);
      Get get = new Get(row.getBytes());

      Result rs = table.get(get);
      for (Cell cell : rs.rawCells()) {
        printlnCell(cell);
      }

      table.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Scan table records
   *
   * @param tableName Table name
   */
  public void scanAllRecord(String tableName) {
    try {
      TableName name = TableName.valueOf(tableName);

      Table table = connection.getTable(name);
      Scan scan = new Scan();
      ResultScanner rs = table.getScanner(scan);

      for (Result result : rs) {
        for (Cell cell : result.rawCells()) {
          printlnCell(cell);
        }
      }

      table.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Scan table records
   *
   * @param tableName Table name
   * @param prefix rowkey prefix
   */
  public void scanRecord(String tableName, String prefix) {
    StopWatch watch = new StopWatch();
    try {
      watch.start();
      TableName name = TableName.valueOf(tableName);

      Table table = connection.getTable(name);
      Scan scan = new Scan();
      scan.setBatch(1000);
      scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));
      ResultScanner rs = table.getScanner(scan);

      for (Result result : rs) {
        for (Cell cell : result.rawCells()) {
          printlnCell(cell);
        }
      }

      table.close();
      watch.stop();
    } catch (IOException e) {
      e.printStackTrace();
    }

    log.info("Execute time: " + watch.getTime());
  }

  /**
   * Scan table records
   *
   * @param tableName Table name
   * @param column column name prefix
   * @param value column value
   */
  public void scanRecord(String tableName, String column, String value) {
    StopWatch watch = new StopWatch();
    try {
      watch.start();
      TableName name = TableName.valueOf(tableName);

      Table table = connection.getTable(name);
      Scan scan = new Scan();
      scan.setBatch(100);
      scan.setFilter(new ColumnPrefixFilter(Bytes.toBytes(column)));
      if (StringUtils.isEmpty(value)) {
        scan.setFilter(new ValueFilter(CompareOp.EQUAL, new SubstringComparator(value)));
      }
      ResultScanner rs = table.getScanner(scan);

      for (Result result : rs) {
        for (Cell cell : result.rawCells()) {
          printlnCell(cell);
        }
      }

      table.close();
      watch.stop();
    } catch (IOException e) {
      e.printStackTrace();
    }

    log.info("Execute time: " + watch.getTime());
  }

  /**
   * Delete by row key
   *
   * @param tableName Table name
   * @param row Row key
   */
  public void delete(String tableName, String row) {

    try {
      TableName name = TableName.valueOf(tableName);
      Table table = connection.getTable(name);

      Delete delete = new Delete(Bytes.toBytes(row));
      table.delete(delete);

      table.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void printlnCell(Cell cell) {
    log.info(Bytes.toString(CellUtil.cloneRow(cell)) + " ");
    log.info(Bytes.toString(CellUtil.cloneFamily(cell)) + ":");
    log.info(Bytes.toString(CellUtil.cloneQualifier(cell)) + " ");
    log.info(Bytes.toString(CellUtil.cloneValue(cell)));
    log.info(cell.getTimestamp() + " ");
  }

}