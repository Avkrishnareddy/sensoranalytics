package com.shashi.spark.streaming.dataloader

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame}

import scala.util.Try

/**
 * Created by shashidhar on 17/3/16.
 */
class CassandraLoader extends DataLoader{
  override def getData(option: Map[String, String], sparkContext: SparkContext): Try[DataFrame] = {
    val tableName = option.get("table").get
    val keyspace = option.get("keyspace").get
    val hc = SQLContext.getOrCreate(sparkContext)
    Try(hc.read.format("org.apache.spark.sql.cassandra").
      options(Map( "table" -> tableName, "keyspace" -> keyspace)).load())
  }
}
