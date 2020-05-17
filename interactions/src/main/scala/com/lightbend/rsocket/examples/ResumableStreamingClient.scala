package com.lightbend.rsocket.examples

import java.time.Duration

import io.rsocket._
import io.rsocket.core._
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.ByteBufPayload
import org.slf4j.LoggerFactory
import reactor.core.publisher._
import reactor.util.retry.Retry

object ResumableStreamingClient {

  private val logger = LoggerFactory.getLogger(this.getClass)

  val RESUME_SESSION_DURATION: Duration = Duration.ofSeconds(60)

  def main(args: Array[String]): Unit = {
    // Ensure clean disposal
    Hooks.onErrorDropped((t: Throwable) => {})

    // Resume configuration
    val resume =
      new Resume()
        .sessionDuration(RESUME_SESSION_DURATION)
        .retry(
          Retry.fixedDelay(Long.MaxValue, Duration.ofSeconds(1))
            .doBeforeRetry(s => logger.info("Disconnected. Trying to resume...")));

    // Create a server
    RSocketServer.create(SocketAcceptor.forRequestStream((payload: Payload) => {
        // Log request
        logger.info(s"Received 'request stream' request with payload: [${payload.getDataUtf8}] ")
        payload.release()
        // return stream
        Flux.interval(Duration.ofMillis(500))
          .map(t => ByteBufPayload.create(t.toString()))
      }))
      .resume(resume)
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(TcpServerTransport.create("0.0.0.0", 7000)).subscribe

    // Create client
    val socket = RSocketConnector.create()
      .resume(resume)
      .connect(TcpClientTransport.create("0.0.0.0", 7001))
      .block

    // Send messages
    socket
      .requestStream(ByteBufPayload.create("Hello"))
      .subscribe((value: Payload) =>  logger.info(s"New stream element ${value.getDataUtf8}"))

    // Wait for completion
    Thread.sleep(100000)
    socket.dispose();
  }
}