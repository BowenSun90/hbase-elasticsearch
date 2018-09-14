package com.alex.space.elastic.function;

import com.alex.space.elastic.utils.ElasticUtils;

/**
 * ElasticSearch query code
 *
 * @author Alex Created by Alex on 2018/9/10.
 */
public class ElasticQuery extends BaseElasticAction {

  public ElasticQuery(String indexName, String typeName, int offset, int insertSize,
      int batchSize) {
    super(indexName, typeName, offset, insertSize, batchSize);
  }

  @Override
  public void run() {

    // TODO
    for (int i = 0; i < 10000; i++) {
      String field = "";
      String value = "";
      String[] showFields = new String[]{};
      ElasticUtils.queryData(indexName, typeName, showFields, value, field);

    }

  }
}
