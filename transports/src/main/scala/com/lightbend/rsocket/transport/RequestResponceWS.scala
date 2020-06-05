package com.lightbend.rsocket.transport

import io.rsocket._
import io.rsocket.core._
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.transport.netty.client._
import io.rsocket.transport.netty.server._
import io.rsocket.util.DefaultPayload
import reactor.core.publisher.Mono


object RequestResponceWS {

  def main(args: Array[String]): Unit = {

    // Server
    RSocketServer.create(SocketAcceptor.forRequestResponse((payload: Payload) => {
      // Just return payload back
      Mono.just(payload)
    }))
      // Enable Zero Copy
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(WebsocketServerTransport.create("0.0.0.0", 7000))
      .subscribe

    // Client
    val socket = RSocketConnector.create()
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .connect(WebsocketClientTransport.create("localhost", 7000))
      .block

    val n = 1000
    val start = System.currentTimeMillis()
    1 to n foreach  {_ =>
      socket.requestResponse(DefaultPayload.create("Hello"))
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
}