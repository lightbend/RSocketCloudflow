package com.lightbend.sensordata.producer.rsocket

import java.time._

import com.lightbend.rsocket.dataconversion.{SensorDataConverter, SensorDataGenerator}
import io.rsocket._
import io.rsocket.core.RSocketConnector
import io.rsocket.transport.netty.client.WebsocketClientTransport
import io.rsocket.util.DefaultPayload
import reactor.core.publisher.{Flux, Mono}

class BinaryRequestStream {

  def run() = RSocketConnector
    .create
    .acceptor(new ClientMessageAcceptor)
    .connect(WebsocketClientTransport.create("0.0.0.0", 3000))
    .block.onClose().block()
}

case class ClientMessageAcceptor() extends SocketAcceptor {
  override def accept(setup: ConnectionSetupPayload, sendingSocket: RSocket): Mono[RSocket] = {
    Mono.just(new BinaryRequestStreamHandler)
  }
}

class BinaryRequestStreamHandler extends AbstractRSocket {

  override def requestStream(payload: Payload): Flux[Payload] = {
    Flux.interval(Duration.ofMillis(1000)).map(_ => DefaultPayload.create(generateData))
  }

  private def generateData: Array[Byte] = {
    SensorDataConverter.toBytes(SensorDataGenerator.random())
  }
}