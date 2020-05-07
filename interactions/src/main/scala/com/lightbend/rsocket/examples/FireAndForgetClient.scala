package com.lightbend.rsocket.examples

import io.rsocket.core.{ RSocketConnector, RSocketServer }
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.DefaultPayload
import io.rsocket.{ AbstractRSocket, ConnectionSetupPayload, Payload, RSocket, SocketAcceptor }
import org.slf4j.LoggerFactory
import reactor.core.publisher.{ Flux, Mono }

object FireAndForgetClient {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    RSocketServer.create(new EchoSocketAcceptorFF())
      .bind(TcpServerTransport.create("localhost", 7000))
      .subscribe

    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("localhost", 7000)).block()

    socket.fireAndForget(DefaultPayload.create("Hello world1!")).block
    socket.fireAndForget(DefaultPayload.create("Hello world2!")).block
    socket.fireAndForget(DefaultPayload.create("Hello world3!")).block
    socket.fireAndForget(DefaultPayload.create("Hello world4!")).block

    Thread.sleep(1000)
    socket.dispose();
  }
}

class EchoSocketAcceptorFF extends SocketAcceptor {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        logger.info(s"Received 'fire-and-forget' request with payload: [${payload.getDataUtf8}]")
        Mono.empty()
      }
    })
}