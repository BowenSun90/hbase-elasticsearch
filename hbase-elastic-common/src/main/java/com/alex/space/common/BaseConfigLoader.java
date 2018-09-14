package com.alex.space.common;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Config properties loader
 *
 * @author Alex Created by Alex on 2018/9/11.
 */
@Slf4j
public abstract class BaseConfigLoader {

  private Properties prop = new Properties();

  protected BaseConfigLoader() {

    try {
      prop.load(this.getClass().getClassLoader().getResourceAsStream("application.properties"));
    } catch (Exception e) {
      log.error("Load config file with exception", e);
    }
  }

  public String getProperty(String key) {
    return prop.getProperty(key);
  }

  public String getProperty(String key, String defaultValue) {
    String value = getProperty(key);
    return StringUtils.isEmpty(value) ? defaultValue : value;
  }

  public Integer getIntProperty(String key) {
    return Integer.parseInt(prop.getProperty(key));
  }

}
