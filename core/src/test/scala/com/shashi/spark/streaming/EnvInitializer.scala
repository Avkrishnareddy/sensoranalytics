package com.shashi.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHive

/**
 * Created by shashidhar on 17/3/16.
 */
trait EnvInitializer {
  Logger.getRootLogger.setLevel(Level.WARN)
  implicit lazy val hiveContext: HiveContext = TestHive
  implicit lazy val sparkContext: SparkContext = hiveContext.sparkContext

}
