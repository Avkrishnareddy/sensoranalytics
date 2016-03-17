package com.shashi.spark.streaming

import org.scalatest.Matchers
import org.scalatest.WordSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

/**
 * Created by shashidhar on 17/3/16.
 */
class SchemaParserTest extends WordSpec with SparkStreamingSpec  with Matchers with Eventually {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(12000, Millis)))

  val windowInterval = 1
  val slideInterval = 1
  "schema parse should" should {

    "return None with wrong record" in {
      val sensorRecordOption = SchemaParser.parse("2015/10/15 00:00:00.449")
      sensorRecordOption.isDefined shouldEqual false
    }
  }
}
