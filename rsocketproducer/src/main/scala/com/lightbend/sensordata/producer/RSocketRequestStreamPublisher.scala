package com.lightbend.sensordata.producer

import java.time.{ Duration, Instant }
import java.util.UUID

import cloudflow.examples.sensordata.rsocket.avro.{ Measurements, SensorData }
import com.lightbend.sensordata.support.{ SensorDataConverter, SensorDataSerializer }
import io.rsocket.core.RSocketConnector
import io.rsocket.{ AbstractRSocket, ConnectionSetupPayload, Payload, RSocket, RSocketFactory, SocketAcceptor }
import io.rsocket.transport.netty.client.{ TcpClientTransport, WebsocketClientTransport }
import io.rsocket.util.DefaultPayload
import reactor.core.publisher.{ Flux, Mono }

class RSocketRequestStreamPublisher {
  val socket = RSocketConnector
    .create()
    .acceptor(new ClientMessageAcceptor)
    .connect(TcpClientTransport.create("0.0.0.0", 3000))
    .block
}

class BinaryClientAcceptor extends AbstractRSocket {

  private val random = new scala.util.Random

  def getData: Array[Byte] = {
    val data = new SensorData(UUID.randomUUID(), Instant.ofEpochMilli(System.currentTimeMillis()),
      new Measurements(random.nextInt(1000) / 10.0, random.nextInt(20000) / 100.0, random.nextInt(2000) / 10.0))
    SensorDataConverter.toBytes(data)
  }

  val neverEnding: Iterator[Array[Byte]] = Stream.from(0).map(cnt => getData).toIterator

  def func: Function[Long, Payload] = (aLong: Long) => {
    val byteArray = neverEnding.next
    DefaultPayload.create(byteArray)
  }

  override def requestStream(payload: Payload): Flux[Payload] = {
    println("Stream Requested")
    Flux.interval(Duration.ofMillis(1000)).map(func(_))
  }
}

case class ClientMessageAcceptor() extends SocketAcceptor {
  override def accept(setup: ConnectionSetupPayload, sendingSocket: RSocket): Mono[RSocket] = Mono.just(new BinaryClientAcceptor)
}
