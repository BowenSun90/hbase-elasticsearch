package com.alex.space.sync.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SparkHBaseContext(@transient val sc: SparkContext)
  extends Serializable {

  def scanHbaseRDD[T: ClassTag](tableName: String,
                                scan: Scan,
                                f: ((ImmutableBytesWritable, Result)) => T): RDD[T] = {

    val job = getHbaseBulkJob(tableName)

    TableMapReduceUtil.initTableMapperJob(tableName,
      scan,
      classOf[IdentityTableMapper],
      null,
      null,
      job)

    sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
      .map(f)
  }

  def getHbaseBulkJob(tableName: String): Job = {
    val conf = HbaseFactory.getConf
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    new Job(conf)
  }

  def hbaseRDD[T: ClassTag](tableName: String,
                            f: ((ImmutableBytesWritable, Result)) => T): RDD[T] = {

    val job = getHbaseBulkJob(tableName)

    sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
      .map(f)
  }

  def hbaseRDD[T: ClassTag](tableName: String,
                            conf: Configuration,
                            f: ((ImmutableBytesWritable, Result)) => T): RDD[T] = {

    val job: Job = new Job(conf)

    sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
      .map(f)
  }


}