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
import reactor.core.publisher.{BaseSubscriber, Flux, Hooks, Mono}
import java.time.Duration

import org.reactivestreams.Subscription

object StreamingClient {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // Ensure clean disposal
    Hooks.onErrorDropped((t: Throwable) => {})

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
      .subscribe(new BaseSubscriber[Payload]{
        // Back pressure subscriber
        private val log = LoggerFactory.getLogger(this.getClass)
        val NUMBER_OF_REQUESTS_TO_PROCESS = 5l
        var receivedItems = 0
        // Start subscribtion
        override def hookOnSubscribe(subscription: Subscription): Unit = {
          subscription.request(NUMBER_OF_REQUESTS_TO_PROCESS)
        }
        // Processing request
        override def hookOnNext(value: Payload): Unit = {
          log.info(s"New stream element ${value.getDataUtf8}")
          receivedItems += 1
          if (receivedItems % NUMBER_OF_REQUESTS_TO_PROCESS == 0) {
            log.info(s"Requesting next [$NUMBER_OF_REQUESTS_TO_PROCESS] elements")
            request(NUMBER_OF_REQUESTS_TO_PROCESS)
          }
        }
        // Invoked on stream completion
        override def hookOnComplete(): Unit = log.info("Completing subscription")
        // Invoked on stream error
        override def hookOnError(throwable: Throwable): Unit = log.error(s"Stream subscription error [$throwable]")
        // Invoked on stream cancelation
        override def hookOnCancel(): Unit = log.info("Subscription canceled")
      })

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
