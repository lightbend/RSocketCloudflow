package com.lightbend.rsocket.transport.ipc

import io.rsocket._
import io.rsocket.core._
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.util.DefaultPayload
import reactor.core.publisher.Mono


object RequestResponceIPC {

  private val length = 1024
  private val directory = "tmp/boris"


  def main(args: Array[String]): Unit = {

    // Server
    RSocketServer.create(SocketAcceptor.forRequestResponse((payload: Payload) => {
      // Just return payload back
      Mono.just(payload)
    }))
      .bind(IPCServerTransport.create(directory))
      .subscribe

    // Client
    val socket = RSocketConnector.create()
      .connect(IPCClientTransport.create(directory))
      .block

    val n = 1000
    val data = repeatChar('x', length)
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