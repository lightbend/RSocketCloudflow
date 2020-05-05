package com.lightbend.sensordata.producer

import java.time.Instant
import java.util.UUID

import cloudflow.examples.sensordata.rsocket.avro._
import com.lightbend.sensordata.support.AvroSerde
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import io.rsocket.core.RSocketConnector
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util.DefaultPayload

import scala.util.Random

object RSocketPublisher {

  private val sensorRecordInjection: Injection[SensorData, Array[Byte]] = SpecificAvroCodecs.toBinary(SensorData.SCHEMA$)
  private val sensorAvroSerde: AvroSerde[SensorData] = new AvroSerde(sensorRecordInjection)

  val random = new Random()

  def main(args: Array[String]): Unit = {
    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("localhost", 3000))
      .block

    while(true){
      Thread.sleep(100)
      socket.fireAndForget(DefaultPayload.create(getData())).block
    }
  }

  def getData(): Array[Byte] = {
    val data = SensorData(UUID.randomUUID(), Instant.ofEpochMilli(System.currentTimeMillis()),
      new Measurements(random.nextInt(1000) / 10.0, random.nextInt(20000) / 100.0, random.nextInt(2000) / 10.0))
    sensorAvroSerde.encode(data)
  }

}
