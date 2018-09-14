package com.alex.space.sync

import com.alex.space.sync.config.{Configable, Constants}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf

import scala.reflect.io.File

/**
  * BaseWorker load config
  */
class BaseWorker extends Configable {

  override val prefix: String = "default"

  def sparkConf: SparkConf = env match {
    case Constants.ENV_LOCAL => new SparkConf().setMaster("local[*]").setAppName("Worker")
    case Constants.ENV_PRODUCT => new SparkConf()
  }

  def outputPath: String = env match {
    case Constants.ENV_LOCAL => val path = configString("output")
      File(path).deleteRecursively()
      path
    case Constants.ENV_PRODUCT => val path = configString("output")
      FileSystem.get(new Configuration).delete(new Path(path), true)
      path

  }

}