package com.shashi.spark.streaming.dataloader

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
 * Created by shashidhar on 17/3/16.
 */
class InMemoryDataLoader(datasets:Map[String,DataFrame]) extends DataLoader {
  override def getData(option: Map[String, String], sparkContext: SparkContext): Try[DataFrame] = {
    val tableName = option.get("table").get
    val df = datasets.get(tableName)
    Try(df.get)
  }
}
