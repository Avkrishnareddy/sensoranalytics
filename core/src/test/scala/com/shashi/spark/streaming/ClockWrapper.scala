package com.shashi.spark.streaming

import org.apache.spark.streaming.{StreamingContextWrapper, StreamingContext}

/**
 * Created by shashidhar on 17/3/16.
 */
class ClockWrapper(ssc: StreamingContext) {

  private val manualClock = new StreamingContextWrapper(ssc).manualClock

  def getTimeMillis: Long = manualClock.getTimeMillis()

  def setTime(timeToSet: Long) = manualClock.setTime(timeToSet)

  def advance(timeToAdd: Long) = manualClock.advance(timeToAdd)

  def waitTillTime(targetTime: Long): Long = manualClock.waitTillTime(targetTime)

}