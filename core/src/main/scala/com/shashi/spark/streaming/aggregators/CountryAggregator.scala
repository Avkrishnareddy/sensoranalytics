package com.shashi.spark.streaming.aggregators

import com.shashi.spark.streaming.dataloader.DataLoader
import com.shashi.spark.streaming.{CountryWiseStats, SensorRecord}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by shashidhar on 17/3/16.
 */
class CountryAggregator extends Aggregator[SensorRecord,CountryWiseStats]{
  override def aggregate(dstream: DStream[SensorRecord], windowInterval: Int, slideInterval: Int)
                        (implicit dataLoader: DataLoader): DStream[CountryWiseStats] =
  {
    val countryAggregation = dstream.map(eachRec=>((dateFormat.format(eachRec.dateTime),eachRec.country),1))

    val resultStream = countryAggregation.window(Seconds(windowInterval),Seconds(slideInterval)).groupByKey().mapValues(_.size)

    val joinStream = resultStream.transform(eachRdd=>{
      if(!eachRdd.isEmpty()){
        val sqlContext = SQLContext.getOrCreate(eachRdd.sparkContext)
        val datesList = eachRdd.map{
          case ((date,country),count) => s"'$date'"
        }.distinct().collect().toList.mkString(",")
        val countryList = eachRdd.map{
          case ((date,country),count) => s"'$country'"
        }.distinct().collect().toList.mkString(",")

        val countrystatsdf = sqlContext.read.format("org.apache.spark.sql.cassandra")
          .options(Map( "table" -> "countrystats", "keyspace" -> "sensoranalytics"))
          .load()
        countrystatsdf.registerTempTable("countrystats")
        val existingRdd = sqlContext.sql(s"select * from countrystats where date in ($datesList) and country in ($countryList)").map {
          case Row(date: String, country: String, count: Int) => ((date, country), count)
        }
        eachRdd.union(existingRdd).reduceByKey(_+_)
      }else{
        eachRdd
      }
    })

    joinStream.map{
      case ((date,country),count) => CountryWiseStats(date,country,count)
    }
  }
}
