package com.lightbend.sensordata.producer.rsocket

import java.time._

import com.lightbend.rsocket.dataconversion.{ SensorDataConverter, SensorDataGenerator }
import io.rsocket._
import io.rsocket.core.RSocketConnector
import io.rsocket.transport.netty.client.WebsocketClientTransport
import io.rsocket.util.DefaultPayload
import reactor.core.publisher.{ Flux, Mono }

class BinaryRequestStream(host: String, port: Int, interval: Long) {

  def run() = RSocketConnector
    .create
    .acceptor((setup: ConnectionSetupPayload, sendingSocket: RSocket) => {
      Mono.just(new BinaryRequestStreamHandler(interval))
    })
    .connect(WebsocketClientTransport.create(host, port))
    .block.onClose().block()
}

class BinaryRequestStreamHandler(interval: Long) extends RSocket {

  override def requestStream(payload: Payload): Flux[Payload] = {
    Flux.interval(Duration.ofMillis(interval)).map(_ => DefaultPayload.create(
      SensorDataConverter.toBytes(SensorDataGenerator.random())))
  }
}