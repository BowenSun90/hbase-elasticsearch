package com.alex.space.flink.connector;

import java.io.Serializable;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * @author Alex Created by Alex on 2018/9/14.
 */
public interface ElasticsearchSinkFunction<T> extends Serializable, Function {

  void process(T element, RuntimeContext ctx, RequestIndexer requestIndexer);

}
