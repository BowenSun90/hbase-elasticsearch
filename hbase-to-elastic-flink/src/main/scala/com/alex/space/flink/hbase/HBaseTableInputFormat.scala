package com.alex.space.flink.hbase

import com.alex.space.flink.config.Configable
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.client.{HTable, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration}
import org.slf4j.LoggerFactory

/**
  * @author Alex Created by Alex on 2018/9/12.
  */
class HBaseTableInputFormat(val tableName: String)
  extends TableInputFormat[Tuple2[String, Array[Cell]]] with Configable {

  private val LOG = LoggerFactory.getLogger(classOf[HBaseTableInputFormat])

  override val prefix: String = "hbase"

  override def configure(parameters: Configuration): Unit = {
    table = createTable()
    if (table != null)
      scan = getScanner
  }

  private def createTable(): HTable = {
    LOG.info("Initializing HBaseConfiguration")
    val hConf = HBaseConfiguration.create()
    hConf.setInt("hbase.client.scanner.timeout.period", 6000000)
    hConf.set("hbase.zookeeper.quorum", configString("zookeeper.quorum"))
    hConf.set("hbase.zookeeper.property.clientPort", configString("zookeeper.clientPort"))

    try
      return new HTable(hConf, getTableName)
    catch {
      case e: Exception =>
        LOG.error("Error instantiating a new HTable instance", e)
    }
    null
  }

  override protected def getScanner = new Scan

  override protected def getTableName: String = this.tableName

  override protected def mapResultToTuple(r: Result): Tuple2[String, Array[Cell]] = {
    val tuple = new Tuple2[String, Array[Cell]]
    tuple.setField(Bytes.toString(r.getRow), 0)
    tuple.setField(r.rawCells, 1)
    tuple
  }

  override def close(): Unit = {
    super.close()
  }

}
