package com.lightbend.rsocket.examples

import io.rsocket.core.{RSocketConnector, RSocketServer}
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.{AbstractRSocket, ConnectionSetupPayload, Payload, RSocket, SocketAcceptor}
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

object Multiserver {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val ports = List(7000, 7001, 70002)

  def main(args: Array[String]): Unit = {
    ports.foreach(port =>
      RSocketServer.create(new FFAcceptorImplementation())
        .bind(TcpServerTransport.create("0.0.0.0", port))
        .subscribe
    )

    val rsocketSuppliers = ports.map(port =>
      RSocketConnector
        .connectWith(TcpClientTransport.create("0.0.0.0", port))
        .block
    )



  }
}

class FFAcceptorImplementation extends SocketAcceptor {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        logger.info(s"Received 'fire-and-forget' request with payload: [${payload.getDataUtf8}] on thread ${Thread.currentThread().getName}")
        Mono.empty()
      }
    })
}