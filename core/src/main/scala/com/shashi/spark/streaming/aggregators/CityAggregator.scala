package com.shashi.spark.streaming.aggregators

import com.shashi.spark.streaming.{CityWiseStats, SensorRecord}
import com.shashi.spark.streaming.dataloader.DataLoader
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by shashidhar on 17/3/16.
 */
class CityAggregator extends Aggregator[SensorRecord,CityWiseStats]{
  override def aggregate(dstream: DStream[SensorRecord], windowInterval: Int, slideInterval: Int)
                        (implicit dataLoader: DataLoader): DStream[CityWiseStats] =
  {
    val cityAggregation = dstream.map(eachRec=>((dateFormat.format(eachRec.dateTime),eachRec.city,eachRec.sensorStatus),1))

    val resultStream = cityAggregation.window(Seconds(windowInterval),Seconds(slideInterval)).groupByKey().mapValues(_.size)

    val joinStream = resultStream.transform(eachRdd=>{
      if(!eachRdd.isEmpty()){
        val sqlContext = SQLContext.getOrCreate(eachRdd.sparkContext)
        val datesList = eachRdd.map{
          case ((date,city,status),count) => s"'$date'"
        }.distinct().collect().toList.mkString(",")
        val cityList = eachRdd.map{
          case ((date,city,status),count) => s"'$city'"
        }.distinct().collect().toList.mkString(",")

        val statusList = eachRdd.map{
          case ((date,city,status),count) => s"'$status'"
        }.distinct().collect().toList.mkString(",")

        val statestatsdf = sqlContext.read.format("org.apache.spark.sql.cassandra")
          .options(Map( "table" -> "citystats", "keyspace" -> "sensoranalytics"))
          .load()
        statestatsdf.registerTempTable("citystats")
        val existingRdd = sqlContext.sql(s"select * from citystats where date in ($datesList) and city in ($cityList) and status in ($statusList)").map {
          case Row(date: String, city: String,status: String, count: Int) => ((date, city,status), count)
        }
        eachRdd.union(existingRdd).reduceByKey(_+_)
      }else{
        eachRdd
      }
    })

    joinStream.map{
      case ((date,city,status),count) => CityWiseStats(date,city,status,count)
    }
  }
}
