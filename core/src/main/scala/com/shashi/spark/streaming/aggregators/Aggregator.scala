package com.shashi.spark.streaming.aggregators

import java.text.SimpleDateFormat

import com.shashi.spark.streaming.dataloader.DataLoader
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by shashidhar on 17/3/16.
 */
trait Aggregator[U,T] extends Serializable
{
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def aggregate(dstream:DStream[U], windowInterval:Int, slideInterval:Int)
               (implicit dataLoader: DataLoader):DStream[T]
}
