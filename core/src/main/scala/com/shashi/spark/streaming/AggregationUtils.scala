package com.shashi.spark.streaming

import com.shashi.spark.streaming.aggregators.{CityAggregator, StateAggregator, CountryAggregator}
import com.shashi.spark.streaming.dataloader.DataLoader
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by shashidhar on 17/3/16.
 */
object AggregationUtils {

  def calculateCountryStats(parsedDstream: DStream[SensorRecord], windowInterval: Int, slideInterval: Int)
                             (implicit dataLoader: DataLoader) = {
    new CountryAggregator().aggregate(parsedDstream,windowInterval,slideInterval)
  }

  def calculateStateStats(parsedDstream: DStream[SensorRecord], windowInterval: Int, slideInterval: Int)
                           (implicit dataLoader: DataLoader) = {
    new StateAggregator().aggregate(parsedDstream,windowInterval,slideInterval)
  }

  def calculateCityStats(parsedDstream: DStream[SensorRecord], windowInterval: Int, slideInterval: Int)
                         (implicit dataLoader: DataLoader) = {
    new CityAggregator().aggregate(parsedDstream,windowInterval,slideInterval)
  }

}
