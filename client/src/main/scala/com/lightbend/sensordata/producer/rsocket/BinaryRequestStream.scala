package com.lightbend.sensordata.producer.rsocket

import java.time._

import com.lightbend.rsocket.dataconversion.{SensorDataConverter, SensorDataGenerator}
import io.rsocket._
import io.rsocket.core.RSocketConnector
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.transport.netty.client.WebsocketClientTransport
import io.rsocket.util.{ByteBufPayload, DefaultPayload}
import reactor.core.publisher.{Flux, Mono, SynchronousSink}

class BinaryRequestStream(host: String, port: Int, interval: Long) {

  def run() = RSocketConnector
    .create()
    .payloadDecoder(PayloadDecoder.ZERO_COPY)
    .acceptor((setup: ConnectionSetupPayload, sendingSocket: RSocket) => {
      Mono.just(new BinaryRequestStreamHandler(interval))
    })
    .connect(WebsocketClientTransport.create(host, port))
    .block.onClose().block()
}

class BinaryRequestStreamHandler(interval: Long) extends RSocket {

  override def requestStream(payload: Payload): Flux[Payload] = {
    payload.release()
    Flux.generate[Payload, Int](() => 0, (state: Int, sink: SynchronousSink[Payload]) => {
      if(interval > 0)
        Thread.sleep(interval)
      sink.next(ByteBufPayload.create(SensorDataConverter.toBytes(SensorDataGenerator.random())))
      state + 1
    })
  }
}