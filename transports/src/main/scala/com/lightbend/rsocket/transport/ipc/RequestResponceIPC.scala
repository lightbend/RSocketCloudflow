package com.lightbend.rsocket.transport.ipc

import io.rsocket._
import io.rsocket.core._
import reactor.core.scheduler.Schedulers
import io.rsocket.frame.decoder.PayloadDecoder
import io.netty.buffer.ByteBufAllocator
import io.rsocket.util.DefaultPayload
import reactor.core.publisher._
import io.rsocket.transport.shm._
import java.time.Duration

import org.slf4j.LoggerFactory

object RequestResponceIPC {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    // Scheduler
    val eventLoopGroup = Schedulers.newParallel("shm-event-loop")

    // Server
    RSocketServer.create(SocketAcceptor.forRequestResponse((payload: Payload) => {
      // Log request
      logger.info(s"Received 'request with payload: [${payload.getDataUtf8}] ")
      Mono.just(DefaultPayload.create("Echo:" + payload.getDataUtf8()))
        .doFinally(_ => payload.release())
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

    val n = 50
    val start = System.currentTimeMillis()
    1 to n foreach { i =>
      socket.requestResponse(DefaultPayload.create(s"new request $i"))
        .map((payload: Payload) => {
          logger.info(s"Received 'response with payload: [${payload.getDataUtf8}] ")
          payload.release()
          payload
        })
        .block
    }
    println(s"Executed $n request/replies in ${System.currentTimeMillis() - start} ms")
    socket.dispose()
  }
}