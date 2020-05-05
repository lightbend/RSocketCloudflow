package com.lightbend.rsocket

import io.rsocket._
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.DefaultPayload
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.time.Instant

import org.slf4j.LoggerFactory

object Server {

  val HOST = "localhost"
  val PORT = 7000
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    RSocketFactory.receive
      .acceptor(new HelloWorldSocketAcceptor)
      .transport(TcpServerTransport.create(HOST, PORT))
      .start.subscribe

    logger.info("Server running")
    Thread.currentThread.join()
  }
}

class HelloWorldSocketAcceptor extends SocketAcceptor {

  private val logger = LoggerFactory.getLogger(classOf[HelloWorldSocketAcceptor])

  override def accept(setup: ConnectionSetupPayload, sendingSocket: RSocket): Mono[RSocket] = {

    logger.info(s"Received connection with setup payload: [${setup.getDataUtf8}] and meta-data: [${setup.getMetadataUtf8}]")

    return Mono.just(new AbstractRSocket() {

      override def fireAndForget(payload: Payload): Mono[Void] = {
        logger.info(s"Received 'fire-and-forget' request with payload: [${payload.getDataUtf8}]")
        Mono.empty()
      }

      override def requestResponse(payload: Payload): Mono[Payload] = {
        logger.info(s"Received 'request response' request with payload: [${payload.getDataUtf8}] ")
        Mono.just(DefaultPayload.create("Hello " + payload.getDataUtf8))
      }

      override def requestStream(payload: Payload): Flux[Payload] = {
        logger.info(s"Received 'request stream' request with payload: [${payload.getDataUtf8}] ")
        Flux.interval(Duration.ofMillis(1000)).map(_ => DefaultPayload.create("Hello " + payload.getDataUtf8 + " @ " + Instant.now))
      }

      override def requestChannel(payloads: Publisher[Payload]): Flux[Payload] =
        Flux.from(payloads)
          .map(payload => {
            logger.info(s"Received payload: [${payload.getDataUtf8}]")
            DefaultPayload.create("Hello " + payload.getDataUtf8 + " @ " + Instant.now)
          })
          .subscribeOn(Schedulers.parallel).asInstanceOf[Flux[Payload]]

      override def metadataPush(payload: Payload): Mono[Void] = {
        logger.info(s"Received 'metadata push' request with metadata: [${payload.getMetadataUtf8}]")
        Mono.empty()
      }
    })
  }
}