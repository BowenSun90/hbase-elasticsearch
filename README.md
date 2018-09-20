# hbase-elasticsearch-sync
HBase、ElasticSearch批量读写更新，HBase扫表批量更新Elasticsearch测试

数据规格：
1000+万行，1000+列，多种数据格式

## hbase-project
Hbase插入、更新、查询（批量）

#### 1. 启动参数
参数含义，参考[HBaseInit](https://github.com/BowenSun90/hbase-elasticsearch/blob/master/hbase-project/src/main/java/com/alex/space/hbase/HBaseInit.java)
```
> nohup java -jar hbase-project.jar \
  -itn ${insertThreadNum} \
  -utn ${updateThreadNum} \
  -stn ${selectThreadNum} \
  -table ${tableName} \
  -cf ${cf} \
  -region ${region} \
  -offset ${offset} \
  -is ${insertSize} \
  -us ${updateSize} \
  -ss ${selectSize} \
  -batch ${batch} > hbase-project.log 2>&1 &
```
#### 2. 配置参数
```
# HBase connection
hbase.zookeeper.quorum=${zk.quorum}
hbase.zookeeper.property.clientPort=${zk.port}
hbase.rootdir=${hbase.rootdir}
hbase.fs.defaultFS=${hbase.fs.defaultFS}
hbase.client.scanner.timeout.period=${hbase.client.scanner.timeout.period}
```

#### 3. function
HBase插入 [HBaseInsert](https://github.com/BowenSun90/hbase-elasticsearch/blob/master/hbase-project/src/main/java/com/alex/space/hbase/function/HBaseInsert.java)

HBase更新 [HBaseUpdate](https://github.com/BowenSun90/hbase-elasticsearch/blob/master/hbase-project/src/main/java/com/alex/space/hbase/function/HBaseUpdate.java)

HBase查询 [HBaseSelect](https://github.com/BowenSun90/hbase-elasticsearch/blob/master/hbase-project/src/main/java/com/alex/space/hbase/function/HBaseSelect.java)

HBaseScan [HBaseStatistics](https://github.com/BowenSun90/hbase-elasticsearch/blob/master/hbase-project/src/main/java/com/alex/space/hbase/function/HBaseStatistics.java)
  - HBase Scan
    ```
    HBaseUtils hBaseUtils = HBaseUtils.getInstance();
    hBaseUtils.printScan(tableName, cf);
    ``` 
  - HBase ClientSideScan
    ```
    ClientSideScanner clientSideScanner = new ClientSideScanner();
    clientSideScanner.tableScan(tableName, cf);
    ```

## elasticsearch-project
Elasticsearch插入、更新、查询（批量）

#### 1. 启动参数
参数含义，参考[ElasticInit](https://github.com/BowenSun90/hbase-elasticsearch/blob/master/elasticsearch-project/src/main/java/com/alex/space/elastic/ElasticInit.java)
```
> nohup java -jar elasticsearch-project.jar \
  -itn ${insertThreadNum} \
  -utn ${updateThreadNum} \
  -stn ${selectThreadNum} \
  -index ${index} \
  -type ${type} \
  -offset ${offset} \
  -is ${insertSize} \
  -us ${updateSize} \
  -ss ${selectSize} \
  -batch ${batch} \
  -delete ${deleteExist} > elasticsearch-project.log 2>&1 &
```

#### 2. 配置参数
```
# elastic connection info
es.cluster.name=${es.cluster.name}
es.node.ip=${es.node.ip}
es.node.port=${es.node.port}
```

#### 3. function
ElasticSearch插入 [ElasticInsert](https://github.com/BowenSun90/hbase-elasticsearch/blob/master/elasticsearch-project/src/main/java/com/alex/space/elastic/function/ElasticInsert.java)

ElasticSearch更新 [ElasticUpdate](https://github.com/BowenSun90/hbase-elasticsearch/blob/master/elasticsearch-project/src/main/java/com/alex/space/elastic/function/ElasticUpdate.java)

ElasticSearch查询 [ElasticQuery](https://github.com/BowenSun90/hbase-elasticsearch/blob/master/elasticsearch-project/src/main/java/com/alex/space/elastic/function/ElasticQuery.java)

## hbase-to-elastic-flink
Flink Scan Hbase, 写入Elasticsearch

### flink-elastic-connector
Flink elasticsearch connector 


## hbase-to-elastic-spark
Spark Scan Hbase，写入Elasticsearch