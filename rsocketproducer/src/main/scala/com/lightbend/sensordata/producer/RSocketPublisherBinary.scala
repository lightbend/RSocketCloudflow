package com.lightbend.sensordata.producer

import java.time.Instant
import java.util.UUID
import io.rsocket.core.RSocketConnector
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util.DefaultPayload

import scala.util.Random

import cloudflow.examples.sensordata.rsocket.avro._
import com.lightbend.sensordata.support.SensorDataConverter

object RSocketPublisherBinary {

  val random = new Random()

  def main(args: Array[String]): Unit = {
    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("0.0.0.0", 3000))
      .block

    while (true) {
      Thread.sleep(1000)
      val payload = DefaultPayload.create(generateData())
      socket.fireAndForget(payload).block
    }
  }

  def generateData(): Array[Byte] = {
    val data = new SensorData(UUID.randomUUID(), Instant.ofEpochMilli(System.currentTimeMillis()),
      new Measurements(random.nextInt(1000) / 10.0, random.nextInt(20000) / 100.0, random.nextInt(2000) / 10.0))
    SensorDataConverter.toBytes(data)
  }
}
