package com.shashi.spark.streaming.dataloader

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
 * Created by shashidhar on 17/3/16.
 */
trait DataLoader {
  def getData(option: Map[String,String],sparkContext: SparkContext):Try[DataFrame]
}
