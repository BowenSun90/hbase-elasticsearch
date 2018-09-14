package com.alex.space.connector;

import java.io.Serializable;
import org.elasticsearch.action.ActionRequest;

/**
 * @author Alex Created by Alex on 2018/9/14.
 */
public interface RequestIndexer extends Serializable {

  void add(ActionRequest... actionRequests);
  
}
