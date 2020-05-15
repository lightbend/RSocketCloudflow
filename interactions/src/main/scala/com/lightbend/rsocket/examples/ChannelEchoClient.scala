package com.lightbend.rsocket.examples

import io.rsocket.{AbstractRSocket, ConnectionSetupPayload, Payload, RSocket, SocketAcceptor}
import io.rsocket.core.RSocketConnector
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.ByteBufPayload
import java.time._

import io.rsocket.frame.decoder.PayloadDecoder
import org.reactivestreams._
import org.slf4j.LoggerFactory
import reactor.core.publisher._

object ChannelEchoClient {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    // Ensure clean disposal
    Hooks.onErrorDropped((t: Throwable) => {})

    // Create server
    RSocketServer.create(SocketAcceptor.forRequestChannel((payloads: Publisher[Payload]) => {
        // For every request
        Flux.from(payloads)
          .map(payload => {
            // Log request
            val pdata = payload.getDataUtf8
            logger.info(s"Received payload: [$pdata]")
            // Send reply
            payload.release()
            ByteBufPayload.create("Echo: " + pdata)
          })
      }))
      // Enable Zero Copy
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(TcpServerTransport.create("0.0.0.0", 7000)).block

    // Create client
    val socket = RSocketConnector.create()
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .connect(TcpClientTransport.create("0.0.0.0", 7000))
      .block

    // Request/responce
    socket
      .requestChannel(
        Flux.interval(Duration.ofMillis(100)).map(_ => {
          val payLoad = ByteBufPayload.create("Hello")
          logger.info(s"Sending payload: [${payLoad.getDataUtf8}]")
          payLoad
        }))
      .subscribe(new BaseSubscriber[Payload] {
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
          value.release()
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

    Thread.sleep(10000)

    socket.dispose();
  }
}