package com.lightbend.rsocket.transport

import io.rsocket._
import io.rsocket.core._
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.transport.netty.client._
import io.rsocket.transport.netty.server._
import io.rsocket.util.DefaultPayload
import reactor.core.publisher.Mono

object RequestResponceTCP {

  val length = 1024

  def main(args: Array[String]): Unit = {

    // Server
    RSocketServer.create(SocketAcceptor.forRequestResponse((payload: Payload) => {
      // Just return payload back
      Mono.just(payload)
    }))
      // Enable Zero Copy
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(TcpServerTransport.create("0.0.0.0", 7000))
      .subscribe

    // Client
    val socket = RSocketConnector.create()
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .connect(TcpClientTransport.create("localhost", 7000))
      .block

    val n = 10000
    val data = repeatChar('x', length).getBytes()
    val start = System.currentTimeMillis()
    1 to n foreach  {_ =>
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
  def repeatChar(char:Char, n: Int) = List.fill(n)(char).mkString
}