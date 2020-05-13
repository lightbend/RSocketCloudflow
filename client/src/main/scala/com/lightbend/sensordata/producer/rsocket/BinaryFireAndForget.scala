package com.lightbend.sensordata.producer.rsocket

import com.lightbend.rsocket.dataconversion.{ SensorDataConverter, SensorDataGenerator }
import io.rsocket.core.RSocketConnector
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util.DefaultPayload

class BinaryFireAndForget(host: String, port: Int, interval: Long) {

  def run(): Unit = {

    // Create client
    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create(host, port))
      .block

    // Send messages
    while (true) {
      Thread.sleep(interval)
      val payload = DefaultPayload.create(generateData())
      socket.fireAndForget(payload).block
    }
  }

  // Generate data
  def generateData(): Array[Byte] = {
    SensorDataConverter.toBytes(SensorDataGenerator.random())
  }
}
