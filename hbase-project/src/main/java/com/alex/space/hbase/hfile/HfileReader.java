package com.alex.space.hbase.hfile;

import com.alex.space.hbase.config.HBaseConfig;
import com.alex.space.hbase.config.HBaseConstants;
import com.alex.space.hbase.utils.HBaseUtils;
import java.io.DataInput;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV3;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * @author Alex Created by Alex on 2018/9/19.
 */
@Slf4j
public class HfileReader {

  private static HBaseConfig hBaseConfig = HBaseConfig.getInstance();

  private Configuration conf;

  private Connection connection;

  private ExecutorService pool = Executors.newScheduledThreadPool(10);

  public HfileReader() {
    try {
      conf = HBaseConfiguration.create();

      conf.set("hbase.zookeeper.quorum", hBaseConfig.getProperty(HBaseConstants.ZOOKEEPER_QUORUM));
      conf.set("hbase.zookeeper.property.clientPort",
          hBaseConfig.getProperty(HBaseConstants.ZOOKEEPER_PORT));

      conf.set("hbase.rootdir", hBaseConfig.getProperty(HBaseConstants.ROOT_DIR));
      conf.set("fs.defaultFS", hBaseConfig.getProperty(HBaseConstants.DEFAULT_FS));

      connection = ConnectionFactory.createConnection(conf, pool);

    } catch (Exception e) {
      log.error(e.getMessage());
    }
  }

  /**
   * 解析布隆过滤器
   */
  public void readBloom(String hfilePath) throws Exception {
    Path path = new Path(hfilePath);
    FileSystem fs = FileSystem.get(conf);
    CacheConfig cacheConfig = new CacheConfig(conf);

    // HFile reader
    Reader reader = HFile.createReader(fs, path, cacheConfig, conf);

    // 创建通用布隆过滤器
    DataInput bloomMeta = reader.getGeneralBloomFilterMetadata();
    BloomFilter bloomFilter = null;
    if (bloomMeta != null) {
      bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);
      log.info(bloomFilter.toString());
    }

    //创建删除的布隆过滤器
    bloomMeta = reader.getDeleteBloomFilterMetadata();
    if (bloomMeta != null) {
      bloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader);
      log.info(bloomFilter.toString());
    }

  }

  /**
   * Scanner读取数据块内容
   */
  public void readScan(String hfilePath) throws Exception {
    Path path = new Path(hfilePath);
    FileSystem fs = FileSystem.get(conf);
    CacheConfig cacheConfig = new CacheConfig(conf);

    FSDataInputStream fsdis = fs.open(path);
    HFileSystem hfs = new HFileSystem(fs);
    FileStatus fileStatus = fs.getFileStatus(path);
    log.info("File status: {}", fileStatus);

    // 由读取流，文件长度，就可以读取到trailer
    FixedFileTrailer trailer = FixedFileTrailer.readFromStream(fsdis, fileStatus.getLen());

    FSDataInputStreamWrapper streamWrapper = new FSDataInputStreamWrapper(fsdis);
    HFileReaderV3 readerV3 = new HFileReaderV3(path, trailer, streamWrapper, fileStatus.getLen(),
        cacheConfig, hfs, conf);

    // 读取FileInfo
    FileInfo fileInfo = readerV3.loadFileInfo();
    for (final Entry<byte[], byte[]> entry : fileInfo.entrySet()) {
      log.info(Bytes.toString(entry.getKey()));
    }

    // Scanner遍历所有的数据块中的KeyValue
    HFileScanner scanner = readerV3.getScanner(false, false);
    scanner.seekTo();

    Cell cell;

    while (scanner.next()) {
      cell = scanner.getKeyValue();
      String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
      HBaseUtils.printCell(rowKey, cell);
    }

    readerV3.close();

  }

}
