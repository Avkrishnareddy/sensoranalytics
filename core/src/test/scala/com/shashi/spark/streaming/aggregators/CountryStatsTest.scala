package com.shashi.spark.streaming.aggregators

import com.shashi.spark.streaming.{CountryWiseStats, AggregationUtils, SchemaParser, SparkStreamingSpec}
import com.shashi.spark.streaming.dataloader.InMemoryDataLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable

/**
 * Created by shashidhar on 17/3/16.
 */
class CountryStatsTest extends WordSpec with SparkStreamingSpec  with Matchers with Eventually {

  val windowInterval = 1
  val slideInterval = 1

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(12000, Millis)))

  "aggregator should" should  {

    "test country stats" in  {
      val sqlContext = new SQLContext(ssc.sparkContext)
      val lines = mutable.Queue[RDD[String]]()

      val dStream = ssc.queueStream(lines,true)
      val parsedDstream = dStream.map(SchemaParser.parse(_)).filter(_!=None).map(_.get)

      val countryStatsSchema = StructType(List(StructField("date",StringType),
        StructField("country",StringType),StructField("count",IntegerType)))

      val countryStatsData = List(List("2016-03-15 07:00:00","India",10)).map(value => Row.fromSeq(value))
      val countryStatsDF = sqlContext.createDataFrame(ssc.sparkContext.makeRDD(countryStatsData), countryStatsSchema)

      val map = Map("countrystats" -> countryStatsDF)
      implicit val inMemoryDataLoader = new InMemoryDataLoader(map)

      val resultantDStream = AggregationUtils.calculateCountryStats(parsedDstream,windowInterval,slideInterval)


      val collector = mutable.MutableList[CountryWiseStats]()

      resultantDStream.foreachRDD{rdd => collector ++= rdd.collect()}

      ssc.start()

      val textRDD = ssc.sparkContext.textFile("core/src/main/resources/source/test.csv")
      lines += textRDD

      clock.advance(batchDuration.milliseconds)

      eventually{
        collector.length shouldEqual 1
        collector.filter(value => {
          value.country=="India" && value.date=="2016-03-15 07:00:00"
        }).head.count shouldEqual 1+39
      }
    }
  }
}
