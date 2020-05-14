package com.lightbend.sensordata.producer.rsocket

import com.lightbend.rsocket.dataconversion.{ SensorDataConverter, SensorDataGenerator }
import io.rsocket.core.RSocketConnector
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util.ByteBufPayload

class BinaryFireAndForget(host: String, port: Int, interval: Long) {

  def run(): Unit = {

    // Create client
    val socket = RSocketConnector.create()
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .connect(TcpClientTransport.create(host, port))
      .block

    // Send messages
    while (true) {
      if (interval > 0)
        Thread.sleep(interval)
      socket.fireAndForget(ByteBufPayload.create(generateData())).block
    }
  }

  // Generate data
  def generateData(): Array[Byte] = {
    SensorDataConverter.toBytes(SensorDataGenerator.random())
  }
}
