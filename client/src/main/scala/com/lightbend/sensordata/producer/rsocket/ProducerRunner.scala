package com.lightbend.sensordata.producer.rsocket

import scala.io.StdIn

object ProducerRunner extends App {
  while (true) {
    print(
      """
        |Select an option:
        |1) Fire and Forget Binary
        |2) Fire and forget UTF8
        |3) Request / Stream Binary
        |
        |>> """.stripMargin)

    StdIn.readInt() match {
      case 1 => new BinaryFireAndForget().run()
      case 2 => new UTF8FireAndForget().run()
      case 3 => new BinaryRequestStream().run()
      case _ => System.exit(0)
    }
  }
}
