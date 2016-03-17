package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

/**
 * Created by shashidhar on 17/3/16.
 */
class StreamingContextWrapper(ssc: StreamingContext) {
  val manualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]
}
