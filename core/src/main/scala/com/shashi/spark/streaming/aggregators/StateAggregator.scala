package com.shashi.spark.streaming.aggregators

import com.shashi.spark.streaming.dataloader.DataLoader
import com.shashi.spark.streaming.{StateWiseStats, SensorRecord}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by shashidhar on 17/3/16.
 */
class StateAggregator extends Aggregator[SensorRecord,StateWiseStats]{
  override def aggregate(dstream: DStream[SensorRecord], windowInterval: Int, slideInterval: Int)
                        (implicit dataLoader: DataLoader): DStream[StateWiseStats] =
  {
    val stateAggregation = dstream.map(eachRec=>((dateFormat.format(eachRec.dateTime),eachRec.state),1))

    val resultStream = stateAggregation.window(Seconds(windowInterval),Seconds(slideInterval)).groupByKey().mapValues(_.size)

    val joinStream = resultStream.transform(eachRdd=>{
      if(!eachRdd.isEmpty()){
        val sqlContext = SQLContext.getOrCreate(eachRdd.sparkContext)
        val datesList = eachRdd.map{
          case ((date,state),count) => s"'$date'"
        }.distinct().collect().toList.mkString(",")
        val stateList = eachRdd.map{
          case ((date,state),count) => s"'$state'"
        }.distinct().collect().toList.mkString(",")

        val options = Map("keyspace" -> "sensoranalytics", "table" -> "statestats")
        val statestatsdf = dataLoader.getData(options,eachRdd.sparkContext).get

        statestatsdf.registerTempTable("statestats")
        val existingRdd = sqlContext.sql(s"select * from statestats where date in ($datesList) and state in ($stateList)").map {
          case Row(date: String, state: String, count: Int) => ((date, state), count)
        }
        eachRdd.union(existingRdd).reduceByKey(_+_)
      }else{
        eachRdd
      }
    })

    joinStream.map{
      case ((date,state),count) => StateWiseStats(date,state,count)
    }
  }
}
