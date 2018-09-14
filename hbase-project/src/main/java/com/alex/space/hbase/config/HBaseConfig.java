package com.alex.space.hbase.config;

import com.alex.space.common.BaseConfigLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * HBase Config
 *
 * @author Alex Created by Alex on 2018/9/8.
 */
@Slf4j
public class HBaseConfig extends BaseConfigLoader {

  private static HBaseConfig instance = null;

  public static synchronized HBaseConfig getInstance() {
    if (instance == null) {
      instance = new HBaseConfig();
    }
    return instance;
  }

}
