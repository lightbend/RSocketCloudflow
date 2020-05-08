package com.lightbend.rsocket.examples

import io.rsocket.{ AbstractRSocket, ConnectionSetupPayload, Payload, RSocket, SocketAcceptor }
import io.rsocket.core.RSocketConnector
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.DefaultPayload
import java.time._

import org.reactivestreams._
import org.slf4j.LoggerFactory
import reactor.core.publisher._

object ChannelEchoClient {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    // Ensure clean disposal
    Hooks.onErrorDropped((t: Throwable) => {})

    // Create server
    RSocketServer.create(new SocketAcceptorImpl())
      .bind(TcpServerTransport.create("localhost", 7000)).block

    // Create client
    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("localhost", 7000))
      .block

    // Back preassure
    val backPressureSubscriber = new BackPressureSubscriber()

    // Request/responce
    socket
      .requestChannel(
        Flux.interval(Duration.ofMillis(100)).map(_ => {
          val p = DefaultPayload.create("Hello")
          logger.info(s"Sending payload: [${p.getDataUtf8}]")
          p
        }))
      .subscribe(backPressureSubscriber);

    Thread.sleep(10000)

    backPressureSubscriber.dispose()
    socket.dispose();
  }
}

class SocketAcceptorImpl extends SocketAcceptor {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def requestChannel(payloads: Publisher[Payload]): Flux[Payload] =
        // For every request
        Flux.from(payloads)
          .map(payload => {
            // Log request
            logger.info(s"Received payload: [${payload.getMetadataUtf8}]")
            // Send reply
            DefaultPayload.create("Echo: " + payload.getMetadataUtf8)
          })
    })
}