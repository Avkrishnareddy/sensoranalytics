package com.shashi.spark.streaming

import java.text.SimpleDateFormat

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.shashi.spark.streaming.dataloader.CassandraLoader
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingMain {


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

    val streamingContext = new StreamingContext(sparkConf,Seconds(batchTime))

    val hc = SQLContext.getOrCreate(streamingContext.sparkContext)


    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      streamingContext, kafkaParams, topicsSet)

    val data = messages.map(_._2)

    val parsedDstream = data.map(SchemaParser.parse(_)).filter(_!=None).map(_.get)

    implicit val dataloader = new CassandraLoader()


    val countryStats = AggregationUtils.calculateCountryStats(parsedDstream,windowTime,slideTime)
    val stateStats = AggregationUtils.calculateStateStats(parsedDstream,windowTime,slideTime)
    val cityStats = AggregationUtils.calculateCityStats(parsedDstream,windowTime,slideTime)

    countryStats.saveToCassandra("sensoranalytics","countrystats",SomeColumns("date","country","count"))
    stateStats.saveToCassandra("sensoranalytics","statestats",SomeColumns("date","state","count"))
    cityStats.saveToCassandra("sensoranalytics","citystats",SomeColumns("date","city","status","count"))

    streamingContext.start()

    streamingContext.awaitTermination()

  }

}

