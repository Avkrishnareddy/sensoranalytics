package com.shashi.spark.streaming

import java.nio.file.Files

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{Suite, BeforeAndAfterAll}

/**
 * Created by shashidhar on 17/3/16.
 */
trait SparkStreamingSpec extends BeforeAndAfterAll {
  this:Suite =>

  private var _ssc: StreamingContext = _

  def ssc = _ssc

  private var _clock: ClockWrapper = _

  def clock = _clock

  val batchDuration = Seconds(1)

  private val master = "local[2]"

  private val appName = this.getClass.getSimpleName

  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  val checkpointDir = Files.createTempDirectory(this.getClass.getSimpleName)


  conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

  override def beforeAll(): Unit = {
    val sc = new SparkContext(conf)
    _ssc = new StreamingContext(sc, batchDuration)
    _ssc.checkpoint(checkpointDir.toString)
    _clock = new ClockWrapper(ssc)
  }

  override def afterAll(): Unit = {
    if (_ssc != null) {
      _ssc.stop(stopSparkContext = false, stopGracefully = false)
      _ssc = null
    }

    super.afterAll()
  }


}
