package com.lightbend.rsocket.examples

import io.rsocket.{AbstractRSocket, ConnectionSetupPayload, Payload, RSocket, SocketAcceptor}
import io.rsocket.core.RSocketConnector
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.DefaultPayload
import java.time._

import org.reactivestreams._
import org.slf4j.LoggerFactory
import reactor.core.publisher.{BaseSubscriber, Flux, Hooks, Mono}

object ChannelEchoClient {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    Hooks.onErrorDropped((t: Throwable) => {})

    RSocketServer.create(new SocketAcceptorImpl())
      .bind(TcpServerTransport.create("localhost", 7000)).block

    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("localhost", 7000))
      .block

    val backPressureSubscriber = new BackPressureSubscriber()

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
        Flux.from(payloads)
          .map(payload => {
            logger.info(s"Received payload: [${payload.getMetadataUtf8}]")
            DefaultPayload.create("Echo: " + payload.getMetadataUtf8)
          })
    })
}

private object BackPressureSubscriber {
  val NUMBER_OF_REQUESTS_TO_PROCESS = 5
}

private class BackPressureSubscriber extends BaseSubscriber[Payload] {

  private val log = LoggerFactory.getLogger(this.getClass)
  var receivedItems = 0

  import BackPressureSubscriber._

  override def hookOnSubscribe(subscription: Subscription): Unit = subscription.request(NUMBER_OF_REQUESTS_TO_PROCESS)

  override def hookOnNext(value: Payload): Unit = {
    receivedItems += 1
    if (receivedItems % NUMBER_OF_REQUESTS_TO_PROCESS == 0) {
      log.info(s"Requesting next [$NUMBER_OF_REQUESTS_TO_PROCESS] elements")
      request(NUMBER_OF_REQUESTS_TO_PROCESS)
    }
  }


  override def hookOnComplete(): Unit = log.info("Completing subscription")

  override def hookOnError(throwable: Throwable): Unit = log.error(s"Stream subscription error [$throwable]")
}
