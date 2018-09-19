package com.alex.space.hbase.hfile;

import com.alex.space.hbase.config.HBaseConfig;
import com.alex.space.hbase.config.HBaseConstants;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV2;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.io.Writable;


/**
 * @author Alex Created by Alex on 2018/9/19.
 */
@Slf4j
public class HfileWriter {

  private static HBaseConfig hBaseConfig = HBaseConfig.getInstance();

  private Configuration conf;

  private Connection connection;

  private ExecutorService pool = Executors.newScheduledThreadPool(10);

  public HfileWriter() {
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

  public void write(String hfilePath) throws IOException {
    //指定写入的路径
    Path path = new Path(hfilePath);
    FileSystem fs = FileSystem.get(conf);
    CacheConfig cacheConfig = new CacheConfig(conf);

    FSDataOutputStream fsdos = fs.create(path);

    // 创建压缩算法，文件块编码，比较器

    // HFile默认的比较器是字典排序的，也可以生成一个自定义的比较器，但必须继承KeyComparator
    Algorithm algorithm = Algorithm.GZ;
    HFileDataBlockEncoder encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.DIFF);
    KVComparator comparator = new KVComparator();
    ChecksumType check = ChecksumType.CRC32;

    // 创建HFile写实现类，指定写入的数据块大小，多少字节生成一个checksum
    int blockSize = 100;
    int checkPerBytes = 16384;
    HFileWriterV2 writerV2 = new HFileWriterV2(conf, cacheConfig, fs, path, fsdos, comparator,
        new HFileContext());

    //创建两个布隆过滤器，指定最大的key数为5
    int maxKey = 5;
    BloomFilterWriter bw1 = BloomFilterFactory
        .createGeneralBloomAtWrite(conf, cacheConfig, BloomType.ROW, maxKey, writerV2);
    BloomFilterWriter bw2 = BloomFilterFactory
        .createDeleteBloomAtWrite(conf, cacheConfig, maxKey, writerV2);

    //生成KeyValue，插入到HFile中，并保存到布隆过滤器中
    long ts = System.currentTimeMillis();
    KeyValue kv = generator("key1", "value", "f", ts, Bytes.toBytes("1"));
    addToHFileWriterAndBloomFile(kv, writerV2, bw1, bw2);

    kv = generator("key2", "value", "f", ts, Bytes.toBytes("2"));
    addToHFileWriterAndBloomFile(kv, writerV2, bw1, bw2);

    kv = generator("key3", "value", "f", ts, Bytes.toBytes("3"));
    addToHFileWriterAndBloomFile(kv, writerV2, bw1, bw2);

    writerV2.addGeneralBloomFilter(bw1);
    writerV2.addDeleteFamilyBloomFilter(bw2);
    writerV2.appendMetaBlock("meta", new Writable() {
      @Override
      public void write(final DataOutput dataOutput) throws IOException {
        dataOutput.write(123456);
      }

      @Override
      public void readFields(final DataInput dataInput) throws IOException {
        dataInput.readInt();
      }
    });
    writerV2.appendFileInfo(Bytes.toBytes("key"), Bytes.toBytes("value"));
    writerV2.close();
  }

  /**
   * 插入一个KeyValue到HFile中，同时将这个key保存到布隆过滤器中
   */
  private void addToHFileWriterAndBloomFile(KeyValue kv, HFileWriterV2 v2, BloomFilterWriter bw,
      BloomFilterWriter bw2)
      throws IOException {
    v2.append(kv);
    byte[] buf = bw.createBloomKey(
        kv.getBuffer(),
        kv.getRowOffset(),
        kv.getRowLength(),
        kv.getBuffer(),
        kv.getQualifierOffset(),
        kv.getQualifierLength());
    bw.add(buf, 0, buf.length);
    bw2.add(buf, 0, buf.length);

  }

  /**
   * 生成KeyValue
   */
  private KeyValue generator(String key, String column, String qualifier, long timestamp,
      byte[] value) {
    byte[] keyBytes = Bytes.toBytes(key);
    byte[] familyBytes = Bytes.toBytes(column);
    byte[] qualifierBytes = Bytes.toBytes(qualifier);
    Type type = Type.Put;
    byte[] valueBytes = value;
    KeyValue kv = new KeyValue(keyBytes, 0, keyBytes.length,
        familyBytes, 0, familyBytes.length,
        qualifierBytes, 0, qualifierBytes.length,
        timestamp, type,
        valueBytes, 0, valueBytes.length);
    return kv;
  }

}
