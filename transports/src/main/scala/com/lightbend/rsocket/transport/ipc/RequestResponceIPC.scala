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

object RequestResponceIPC {

  val length = 1024

  def main(args: Array[String]): Unit = {

    // Scheduler
    val eventLoopGroup = Schedulers.newParallel("shm-event-loop")

    // Server
    RSocketServer.create(SocketAcceptor.forRequestResponse((payload: Payload) => {
      Mono.just(DefaultPayload.create("Echo:" + payload.getDataUtf8()))
        .log()
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

    val n = 5
    val data = repeatChar('x', length).getBytes()
    val start = System.currentTimeMillis()
    1 to n foreach { _ =>
      socket.requestResponse(DefaultPayload.create(data))
        .map((payload: Payload) => {
          //          println(s"Got reply ${payload.getDataUtf8}")
          payload.release()
          payload
        })
        .block
    }
    println(s"Executed $n request/replies in ${System.currentTimeMillis() - start} ms")
    socket.dispose()
  }

  // Create a string of length
  def repeatChar(char: Char, n: Int) = List.fill(n)(char).mkString
}