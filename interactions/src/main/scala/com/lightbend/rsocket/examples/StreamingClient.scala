package com.lightbend.rsocket.examples

import io.rsocket.core.RSocketConnector
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import org.slf4j.LoggerFactory
import io.rsocket.AbstractRSocket
import io.rsocket.ConnectionSetupPayload
import io.rsocket.Payload
import io.rsocket.RSocket
import io.rsocket.SocketAcceptor
import io.rsocket.util.DefaultPayload
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

object StreamingClient {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    // Creat a server
    RSocketServer.create(new EchoSocketAcceptorImpl())
      .bind(TcpServerTransport.create("localhost", 7000)).subscribe

    // create a client
    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("localhost", 7000))
      .block

    // Send messages
    socket
      .requestStream(DefaultPayload.create("Hello"))
      .limitRequest(100)
      .subscribe(new BackPressureSubscriber())

    // Wait for completion
    Thread.sleep(3000)
    socket.dispose();
  }
}

class EchoSocketAcceptorImpl extends SocketAcceptor {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def requestStream(payload: Payload): Flux[Payload] = {
        // Log request
        logger.info(s"Received 'request stream' request with payload: [${payload.getDataUtf8}] ")
        // return stream
        return Flux.interval(Duration.ofMillis(100)).map(aLong => DefaultPayload.create("Interval: " + aLong))
      }
    })
}