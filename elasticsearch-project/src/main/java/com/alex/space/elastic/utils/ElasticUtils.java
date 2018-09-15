package com.alex.space.elastic.utils;

import com.alex.space.common.DataTypeEnum;
import com.alex.space.common.KeyValueGenerator;
import com.alex.space.elastic.config.ElasticConfig;
import com.alex.space.elastic.config.ElasticConstants;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Elastic Utils
 *
 * @author Alex Created by Alex on 2018/9/10.
 */
@Slf4j
public class ElasticUtils {

  private static ElasticConfig elasticConfig = ElasticConfig.getInstance();

  private static TransportClient client;

  /**
   * 初始化连接
   */
  public static void init() throws UnknownHostException {

    //设置集群名称
    Settings settings = Settings.builder()
        .put("cluster.name", elasticConfig.getProperty(ElasticConstants.CLUSTER_NAME)).build();

    //创建client
    client = new PreBuiltTransportClient(settings).addTransportAddress(
        new InetSocketTransportAddress(
            InetAddress.getByName(elasticConfig.getProperty(ElasticConstants.ES_NODE_IP)),
            elasticConfig.getIntProperty(ElasticConstants.ES_NODE_PORT)));
  }

  /**
   * 判断索引是否存在
   *
   * @param index index name
   * @return TRUE-exists
   */
  private static boolean indexExists(String index, String type) {
    IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(index);
    IndicesExistsResponse indicesExistsResponse = client.admin().indices()
        .exists(indicesExistsRequest)
        .actionGet();

    if (!indicesExistsResponse.isExists()) {
      log.info("Index not exists");
      return false;
    }

    TypesExistsRequest typesExistsRequest = new TypesExistsRequest(new String[]{index}, type);
    TypesExistsResponse typesExistsResponse = client.admin().indices()
        .typesExists(typesExistsRequest).actionGet();
    if (!typesExistsResponse.isExists()) {
      log.info("Type not exists");
      return false;
    }

    return true;
  }

  /**
   * 创建索引
   *
   * @param index 索引
   * @param type 类型
   * @param deleteExist 是否删除已经存在的索引
   */
  public static void createIndex(String index, String type, boolean deleteExist)
      throws IOException, InterruptedException {

    // Delete exist index
    if (indexExists(index, type)) {
      log.info("index: {}, type: {} exists", index, type);

      if (deleteExist) {
        deleteIndex(index);
        Thread.sleep(5000);
      } else {
        return;
      }
    }

    // Create index
    CreateIndexRequest request = new CreateIndexRequest(index);
    client.admin().indices().create(request);

    // Create mapping
    XContentBuilder mapping = XContentFactory.jsonBuilder()
        .startObject()
        .startObject("properties");

    for (DataTypeEnum dataTypeEnum : DataTypeEnum.values()) {
      int length = dataTypeEnum == DataTypeEnum.Json ? 0 : 5;
      for (int i = 0; i < length; i++) {
        String field = KeyValueGenerator.generateKey(dataTypeEnum, i);
        mapping.startObject(field)
            .field("type", dataTypeEnum.getEsType());

        if (dataTypeEnum == DataTypeEnum.String || dataTypeEnum == DataTypeEnum.StringArray) {
          mapping.field("index", "not_analyzed");
        }

        mapping.endObject();
      }
    }

    mapping.endObject().endObject();

    PutMappingRequest putMappingRequest =
        Requests.putMappingRequest(index).type(type).source(mapping);

    PutMappingResponse response = client.admin().indices().putMapping(putMappingRequest)
        .actionGet();
    if (response.isAcknowledged()) {
      log.info("Create index successfully.");
      Thread.sleep(5000);
    }
  }

  /**
   * 删除索引
   *
   * @param index index name
   */
  public static void deleteIndex(String index) {

    // 判断索引是否存在
    IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(index);
    IndicesExistsResponse inExistsResponse = client.admin().indices().exists(inExistsRequest)
        .actionGet();
    if (inExistsResponse.isExists()) {
      log.info("Index: " + index + " exists.");

      DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(index).execute()
          .actionGet();
      if (dResponse.isAcknowledged()) {
        log.info("Index: " + index + " delete success.");
      } else {
        log.info("Index: " + index + " delete failed.");
      }
    } else {
      log.info("Index: " + index + " not exists.");
    }
  }

  /**
   * 批量插入
   *
   * @param index 索引
   * @param type 类型
   * @param records 插入记录
   * @param offset 插入起始offset
   */
  public static void bulkInsert(String index, String type, List<String> records, int offset) {
    BulkRequestBuilder bulkRequest = client.prepareBulk();

    for (String jsonData : records) {
      String id = String.valueOf(offset++);
      bulkRequest.add(client.prepareIndex(index, type, id).setSource(jsonData, XContentType.JSON));
    }

    BulkResponse response = bulkRequest.execute().actionGet();
    log.debug("Bulk request add: " + response.status().getStatus());
  }

  /**
   * 批量更新
   *
   * @param index 索引
   * @param type 类型
   * @param records 插入记录
   * @param offset 更新最大offset
   */
  public static void bulkUpdate(String index, String type, List<String> records, int offset) {
    BulkRequestBuilder bulkRequest = client.prepareBulk();

    for (String jsonData : records) {
      String id = String.valueOf(offset++);
      bulkRequest.add(client.prepareUpdate(index, type, id).setDoc(jsonData, XContentType.JSON));
    }

    BulkResponse response = bulkRequest.execute().actionGet();
    log.debug("Bulk request update: " + response.status().getStatus());
  }

  /**
   * Query data by matching column value
   *
   * @param index index name
   * @param type type name
   * @param value column value
   * @param fields field list
   */
  public static void queryData(String index, String type, String[] showField, String value,
      String fields) {

    QueryBuilder query = QueryBuilders.termQuery(fields, value);

    SearchResponse response = client.prepareSearch(index)
        .setTypes(type)
        .setQuery(query)
        .setFetchSource(showField, null)
        .setMinScore(0.8f)
        .execute()
        .actionGet();

    SearchHits hits = response.getHits();
    log.debug("搜到{}条结果", hits.totalHits);
  }

}