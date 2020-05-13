package com.lightbend.sensordata.producer.rsocket

import com.lightbend.rsocket.dataconversion.SensorDataGenerator
import io.rsocket.core.RSocketConnector
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util.DefaultPayload

import scala.util.Random

class UTF8FireAndForget(host: String, port: Int, interval: Long) {

  def run(): Unit = {

    // Create client
    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create(host, port))
      .block

    // Send messages
    while (true) {
      Thread.sleep(interval)
      val payload = DefaultPayload.create(SensorDataGenerator.randomJsonString)
      socket.fireAndForget(payload).block
    }
  }

}
