package com.lightbend.rsocket.examples

import io.rsocket.core.{ RSocketConnector, RSocketServer }
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.DefaultPayload
import io.rsocket._
import org.slf4j.LoggerFactory
import reactor.core.publisher.{ Mono }

object FireAndForgetClient {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    // Create server
    RSocketServer.create((setup: ConnectionSetupPayload, sendingSocket: RSocket) => {
      Mono.just(new RSocket() {
        override def fireAndForget(payload: Payload): Mono[Void] = {
          // Log message
          logger.info(s"Received 'fire-and-forget' request with payload: [${payload.getDataUtf8}]")
          Mono.empty()
        }
      })})
      // Enable Zero Copy
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(TcpServerTransport.create("localhost", 7000))
      .subscribe

    // Create client
    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("localhost", 7000))
      .block()

    // Send some messages
    socket.fireAndForget(DefaultPayload.create("Hello world1!")).block
    socket.fireAndForget(DefaultPayload.create("Hello world2!")).block
    socket.fireAndForget(DefaultPayload.create("Hello world3!")).block
    socket.fireAndForget(DefaultPayload.create("Hello world4!")).block

    // Wit and complete
    Thread.sleep(1000)
    socket.dispose();
  }
}