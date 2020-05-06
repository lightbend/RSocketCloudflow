package com.lightbend.sensordata.producer

import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID

import com.lightbend.sensordata.support.AvroSerde
import cloudflow.examples.sensordata.rsocket.avro._
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import io.rsocket.{AbstractRSocket, ConnectionSetupPayload, Payload, RSocket, SocketAcceptor}
import io.rsocket.core.{RSocketConnector, RSocketServer}
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.DefaultPayload
import reactor.core.publisher.Mono

import scala.util.Random

object TestBinary {

  private val sensorRecordInjection: Injection[SensorData, Array[Byte]] = SpecificAvroCodecs.toBinary(SensorData.SCHEMA$)
  private val sensorAvroSerde: AvroSerde[SensorData] = new AvroSerde(sensorRecordInjection)

  val random = new Random()

  def main(args: Array[String]): Unit = {

    RSocketServer.create(new SocketAcceptorImpl())
      .bind(TcpServerTransport.create("localhost", 7000))
      .subscribe

    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("localhost", 7000))
      .block

    1 to 5 foreach{_ =>
      socket.fireAndForget(DefaultPayload.create(generateData())).block()
    }

    Thread.sleep(1000)
    socket.dispose();
  }

  def generateData(): Array[Byte] = {
    val data = new SensorData(UUID.randomUUID(), Instant.ofEpochMilli(System.currentTimeMillis()),
      new Measurements(random.nextInt(1000) / 10.0, random.nextInt(20000) / 100.0, random.nextInt(2000) / 10.0))
    println(s"Generating data $data")
    sensorAvroSerde.encode(data)
  }

  def convertData(buffer : ByteBuffer): SensorData = {
    val data = new Array[Byte](buffer.remaining())
    buffer.get(data)
    val sensor = sensorAvroSerde.decode(data)
    println(s"Converting data $sensor")
    sensor
  }
}

class SocketAcceptorImpl extends SocketAcceptor {

  import TestBinary._

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        convertData(payload.getData)
        Mono.empty()
      }
    })
}