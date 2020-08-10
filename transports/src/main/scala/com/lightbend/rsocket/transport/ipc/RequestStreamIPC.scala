package com.lightbend.rsocket.transport.ipc

import java.time.Duration

import io.netty.buffer.ByteBufAllocator
import io.rsocket._
import io.rsocket.core._
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.util._
import io.rsocket.transport.shm._
import org.slf4j.LoggerFactory
import reactor.core.publisher._
import reactor.core.scheduler.Schedulers

object RequestStreamIPC {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    // Ensure clean disposal
    Hooks.onErrorDropped((t: Throwable) => {})

    // Scheduler
    val eventLoopGroup = Schedulers.newParallel("shm-event-loop")

    // Server
    RSocketServer.create(SocketAcceptor.forRequestStream((payload: Payload) => {
      // Log request
      logger.info(s"Received 'request stream' request with payload: [${payload.getDataUtf8}] ")
      payload.release()
      // return stream
      Flux.interval(Duration.ofMillis(500))
        .map(t => ByteBufPayload.create(t.toString()))
    }))
      // Enable Zero Copy
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(SharedMemoryServerTransport.create(8081, ByteBufAllocator.DEFAULT, eventLoopGroup))
      .subscribe

    // Client
    val socket = RSocketConnector.create()
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .keepAlive(Duration.ofMillis(Integer.MAX_VALUE), Duration.ofMillis(Integer.MAX_VALUE))
      .connect(SharedMemoryClientTransport.create(8081, ByteBufAllocator.DEFAULT, eventLoopGroup))
      .block

    // Send messages
    socket
      .requestStream(ByteBufPayload.create("Hello"))
      .subscribe((value: Payload) => logger.info(s"New stream element ${value.getDataUtf8}"))

    // Wait for completion
    Thread.sleep(10000)
    socket.dispose();
  }
}