package com.alex.space.flink.core

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes

/**
  * @author Alex Created by Alex on 2018/9/12.
  */
class HBaseTableInputFormat(val tableName: String) extends TableInputFormat[Tuple2[String, Array[Cell]]] {

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
