package com.shashi.spark.streaming

import java.text.SimpleDateFormat

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingMain {

  val sparkConfig = Configuration.sparkConfig

  val streamingConfig = Configuration.streamingConfig


  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def calculateCountryStats(parsedDstream:DStream[SensorRecord],windowInterval:Int,slideInterval:Int)={
    val countryAggregation = parsedDstream.map(eachRec=>((dateFormat.format(eachRec.dateTime),eachRec.country),1))

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
    joinStream.map({
      case ((date,country),count) => CountryWiseStats(date,country,count)
    }).saveToCassandra("sensoranalytics","countrystats",SomeColumns("date","country","count"))
  }

  def calculateStateStats(parsedDstream:DStream[SensorRecord],windowInterval:Int,slideInterval:Int)={
    val stateAggregation = parsedDstream.map(eachRec=>((dateFormat.format(eachRec.dateTime),eachRec.state),1))

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

        val statestatsdf = sqlContext.read.format("org.apache.spark.sql.cassandra")
          .options(Map( "table" -> "statestats", "keyspace" -> "sensoranalytics"))
          .load()
        statestatsdf.registerTempTable("statestats")
        val existingRdd = sqlContext.sql(s"select * from statestats where date in ($datesList) and state in ($stateList)").map {
          case Row(date: String, state: String, count: Int) => ((date, state), count)
        }
        eachRdd.union(existingRdd).reduceByKey(_+_)
      }else{
        eachRdd
      }
    })

    joinStream.map({
      case ((date,state),count) => StateWiseStats(date,state,count)
    }).saveToCassandra("sensoranalytics","statestats",SomeColumns("date","state","count"))
  }

  def calculateCityStats(parsedDstream:DStream[SensorRecord],windowInterval:Int,slideInterval:Int)={
    val cityAggregation = parsedDstream.map(eachRec=>((dateFormat.format(eachRec.dateTime),eachRec.city,eachRec.sensorStatus),1))

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
    joinStream.map({
      case ((date,city,status),count) => CityWiseStats(date,city,status,count)
    }).saveToCassandra("sensoranalytics","citystats",SomeColumns("date","city","status","count"))
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()

    val appname = args(0)
    val batchTime = args(1).toInt
    val windowTime = args(2).toInt
    val slideTime = args(3).toInt
    val topics = args(4)
    val brokers = args(5)
    val cassandraHost = args(6)

    sparkConf.setMaster("local[2]")

    sparkConf.setAppName(appname)

    sparkConf.set("spark.cassandra.connection.host", cassandraHost)

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val ssc = new StreamingContext(sparkConf,Seconds(batchTime))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val data = messages.map(_._2)

    val parsedDstream = data.map(SchemaParser.parse(_)).filter(_!=None).map(_.get)

    calculateCountryStats(parsedDstream,windowTime,slideTime)

    calculateStateStats(parsedDstream,windowTime,slideTime)

    calculateCityStats(parsedDstream,windowTime,slideTime)

    ssc.start()

    ssc.awaitTermination()

  }

}

