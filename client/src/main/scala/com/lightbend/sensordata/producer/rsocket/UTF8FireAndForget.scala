package com.lightbend.sensordata.producer.rsocket

import com.lightbend.sensordata.support.SensorDataGenerator
import io.rsocket.core.RSocketConnector
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util.DefaultPayload

import scala.util.Random

class UTF8FireAndForget {

  def run(): Unit = {

    // Create client
    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("0.0.0.0", 3000))
      .block

    // Send messages
    while (true) {
      Thread.sleep(1000)
      val payload = DefaultPayload.create(SensorDataGenerator.randomJsonString.toString)
      socket.fireAndForget(payload).block
    }
  }

}
