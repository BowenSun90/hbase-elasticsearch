package com.alex.space.sync

import com.alex.space.sync.core.SparkHBaseContext
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkContext

/**
  * hbase spark scan
  *
  * @author Alex Created by Alex on 2018/9/11.
  */
object HbaseScanWorker extends BaseWorker {

  override val prefix: String = "sync"

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(sparkConf)
    val hc = new SparkHBaseContext(sc)

    val tableName = configString("hbase.table")

    // 处理HBase行数据
    def mapRow(r: (ImmutableBytesWritable, Result)) = {
      val result = r._2
      val rowKey = new String(result.getRow)
      val values = result.rawCells()
        .map { cell =>
          val cf = new String(CellUtil.cloneFamily(cell))
          val qualifier = new String(CellUtil.cloneQualifier(cell))
          val value = new String(CellUtil.cloneValue(cell))
          (cf, qualifier, value)
        }

      (rowKey, values)
    }

    // MR scan hbase
    hc.scanHbaseRDD(tableName, new Scan(), mapRow)
      .foreachPartition(iter => {
        var rows = 0
        while (iter.hasNext) {
          val rowData = iter.next()
          println(rowData._1, rowData._2.mkString(","))
          rows += 1
        }

        println("Process rows: " + rows)

      })

  }
}
