package com.alex.space.sync.core

import java.util

import com.alex.space.sync.config.Configable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._

/**
  * @author Alex Created by Alex on 2018/9/10.
  */
object HbaseFactory extends Configable {

  override val prefix: String = "hbase"

  var conn: Connection = _
  val tables: util.HashMap[String, Table] = new util.HashMap[String, Table]

  def getConf: Configuration = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", configString("zookeeper.quorum"))
    hbaseConf.set("hbase.zookeeper.property.clientPort", configString("zookeeper.clientPort"))
    hbaseConf
  }

  def initConn() {
    if (conn == null || conn.isClosed) {
      println("----  Init Conn  -----")
      conn = ConnectionFactory.createConnection(getConf)
    }
  }

  def getConn: Connection = {
    initConn()
    conn
  }

  def getTable(tableName: String): Table = {
    tables.getOrElse(tableName,
      {
        initConn()
        conn.getTable(TableName.valueOf(tableName))
      }
    )
  }

  def put(tableName: String, p: Put) {
    getTable(tableName).put(p)
  }

  def get(tableName: String, get: Get, columns: Array[String]): Array[String] = {
    val r = getTable(tableName).get(get)
    if (r != null && !r.isEmpty) {
      columns.map { x => new String(r.getValue("info".getBytes, x.getBytes)) }
    } else null
  }
}
