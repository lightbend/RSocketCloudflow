package com.lightbend.sensordata.producer.rsocket

import com.lightbend.rsocket.configuration.RSocketConfiguration._

object ProducerRunner {
  def main(args: Array[String]): Unit = {

    println(s"Running Producer runner for host $RSOCKET_HOST, port $RSOCKET_PORT. Sending interval $PRODUCER_INTERVAL")
    PRODUCER_OPTION match {
      case 1 =>
        println(s"Running binary fire and forget producer")
        new BinaryFireAndForget(RSOCKET_HOST, RSOCKET_PORT, PRODUCER_INTERVAL).run()
      case 2 =>
        println(s"Running UTF8 fire and forget producer")
        new UTF8FireAndForget(RSOCKET_HOST, RSOCKET_PORT, PRODUCER_INTERVAL).run()
      case _ =>
        println(s"Running binary stream producer")
        new BinaryRequestStream(RSOCKET_HOST, RSOCKET_PORT, PRODUCER_INTERVAL).run()
    }
  }
}