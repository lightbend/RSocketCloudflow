package com.lightbend.rsocket.examples

import io.rsocket.{ AbstractRSocket, ConnectionSetupPayload, Payload, RSocket, SocketAcceptor }
import io.rsocket.core.RSocketConnector
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.DefaultPayload
import java.time.{ Duration, Instant }

import org.reactivestreams.{ Publisher, Subscriber, Subscription }
import org.slf4j.LoggerFactory
import reactor.core.publisher.{ Flux, Mono }

object ChannelEchoClient {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    RSocketServer.create(new SocketAcceptorImpl())
      .bind(TcpServerTransport.create("localhost", 7000)).subscribe

    val socket = RSocketConnector
      .connectWith(TcpClientTransport.create("localhost", 7000))
      .block

    socket
      .requestChannel(
        Flux.interval(Duration.ofMillis(1000)).map(_ => {
          val p = DefaultPayload.create("Hello")
          logger.info(s"Sending payload: [${p.getDataUtf8}]")
          p
        }))
      .subscribe(new BackPressureSubscriber());

    Thread.sleep(1000)
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

private class BackPressureSubscriber extends Subscriber[Payload] {

  private val log = LoggerFactory.getLogger(this.getClass)
  private var subscription: Subscription = null
  var receivedItems = 0

  import BackPressureSubscriber._

  def onSubscribe(s: Subscription): Unit = {
    this.subscription = s
    subscription.request(NUMBER_OF_REQUESTS_TO_PROCESS)
  }

  def onNext(payload: Payload): Unit = {
    receivedItems += 1
    if (receivedItems % NUMBER_OF_REQUESTS_TO_PROCESS == 0) {
      log.info(s"Requesting next [$NUMBER_OF_REQUESTS_TO_PROCESS] elements")
      subscription.request(NUMBER_OF_REQUESTS_TO_PROCESS)
    }
  }

  def onError(t: Throwable): Unit = {
    log.error(s"Stream subscription error [$t]")
  }

  def onComplete(): Unit = {
    log.info("Completing subscription")
  }
}
