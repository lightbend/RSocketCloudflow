package com.lightbend.sensordata.producer

import java.util.UUID

import io.rsocket.core.RSocketConnector
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util.DefaultPayload

import scala.util.Random

object RSocketPublisher {

  val random = new Random()                           // Random generator

  def main(args: Array[String]): Unit = {

    // Create client
    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("0.0.0.0", 3000))
      .block

    // Send messages
    while(true){
      Thread.sleep(1000)
      val payload = DefaultPayload.create(getData())
      socket.fireAndForget(payload).block
    }
  }

  // Create a JSON message
  def getData(): String =
    s"""{
       |        "deviceId": "${UUID.randomUUID().toString}",
       |        "timestamp": ${System.currentTimeMillis()},
       |        "measurements": {
       |            "power":  ${random.nextInt(1000) / 10.0},
       |            "rotorSpeed": ${random.nextInt(20000) / 100.0},
       |            "windSpeed": ${random.nextInt(2000) / 10.0}
       |         }
       |     }""".stripMargin
}
