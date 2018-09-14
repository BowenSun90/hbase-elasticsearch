package com.alex.space.elastic.config;

import com.alex.space.common.BaseConfigLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * Elastic Config
 *
 * @author Alex Created by Alex on 2018/9/11.
 */
@Slf4j
public class ElasticConfig extends BaseConfigLoader {

  private static ElasticConfig instance = null;

  private ElasticConfig() {
    super();
  }

  public static synchronized ElasticConfig getInstance() {
    if (instance == null) {
      instance = new ElasticConfig();
    }
    return instance;
  }

}
