package com.shashi.spark.streaming

import java.text.SimpleDateFormat


import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime


object StreamingMain {

  val sparkConfig = Configuration.sparkConfig

  val streamingConfig = Configuration.streamingConfig


  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def calculateCountryStats(parsedDstream:DStream[SensorRecord],windowInterval:Int,slideInterval:Int):DStream[String]={
    val countryAggregation = parsedDstream.map(eachRec=>((eachRec.dateTime,eachRec.country),1))

    val resultStream = countryAggregation.window(Seconds(windowInterval),Seconds(slideInterval)).groupByKey().mapValues(_.size)

    resultStream.map{
      case ((dateTime:DateTime,country:String),count:Int) => dateTime.toString("yyyy-MM-dd HH:mm:ss")+","+country+","+count
    }
  }

  def calculateStateStats(parsedDstream:DStream[SensorRecord],windowInterval:Int,slideInterval:Int):DStream[String]={
    val countryAggregation = parsedDstream.map(eachRec=>((eachRec.dateTime,eachRec.state),1))

    val resultStream = countryAggregation.window(Seconds(windowInterval),Seconds(slideInterval)).groupByKey().mapValues(_.size)

    resultStream.map{
      case ((dateTime:DateTime,state:String),count:Int) => dateTime.toString("yyyy-MM-dd HH:mm:ss")+","+state+","+count
    }
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()

    val appname = args(0)
    val batchTime = args(1).toInt
    val windowTime = args(2).toInt
    val slideTime = args(3).toInt

    sparkConf.setMaster("local[2]")

    sparkConf.setAppName(appname)

    val ssc = new StreamingContext(sparkConf,Seconds(batchTime))

    val data = ssc.textFileStream(streamingConfig.getString("source-dir"))

    val parsedDstream = data.map(SchemaParser.parse(_)).filter(_!=None).map(_.get)


    calculateCountryStats(parsedDstream,windowTime,slideTime).foreachRDD((outputRdd,time)=>{
      if(!outputRdd.isEmpty()) outputRdd.saveAsTextFile(streamingConfig.getString("output-dir")+"/countryStats/"+time.toString())
    })

    calculateStateStats(parsedDstream,windowTime,slideTime).foreachRDD((outputRdd,time)=>{
      if(!outputRdd.isEmpty()) outputRdd.saveAsTextFile(streamingConfig.getString("output-dir")+"/stateStats/"+time.toString())
    })

    ssc.start()

    ssc.awaitTermination()

  }

}

