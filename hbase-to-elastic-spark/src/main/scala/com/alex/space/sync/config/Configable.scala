package com.alex.space.sync.config

import com.typesafe.config.Config

/**
  * Base Config
  */
trait Configable {
  
  val config: Config = {
    val baseConfiguration = com.typesafe.config.ConfigFactory.load()
    val envConfiguration = com.typesafe.config.ConfigFactory.load(baseConfiguration.getString("env"))
    envConfiguration.withFallback(baseConfiguration)
  }

  val prefix: String

  val env: String = config.getString("env")

  def configString(key: String): String = configGet(key, (con, key) => con.getString(key))

  def configInt(key: String): Int = configGet(key, (con, key) => con.getInt(key))

  def configGet[T](key: String, op: (Config, String) => T): T = op(config, s"$prefix.$key")

}
