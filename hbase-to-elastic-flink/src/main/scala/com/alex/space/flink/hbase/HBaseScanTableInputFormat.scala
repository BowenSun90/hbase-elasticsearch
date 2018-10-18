package com.alex.space.flink.hbase

import java.util

import com.alex.space.flink.connector.hbase.ScanTableInputFormat
import org.apache.hadoop.hbase.client.Result

/**
  * @author Alex
  *         Created by Alex on 2018/10/18.
  */
class HBaseScanTableInputFormat(tableNameStr: String, cf: String, qualifiers: util.List[String])
  extends ScanTableInputFormat[RowData](tableNameStr, cf, qualifiers) {

  override def mapResultToOutType(r: Result): RowData = {
    val rowData = new RowData
    rowData.setRow(Bytes.toString(r.getRow))
    val map = new util.HashMap[String, String](256)
    for (cell <- r.rawCells) {
      map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)))
    }
    rowData.setKeyValue(map)
    return rowData
  }
}
