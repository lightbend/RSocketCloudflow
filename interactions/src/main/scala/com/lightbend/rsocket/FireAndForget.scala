package com.lightbend.rsocket

import io.rsocket.RSocketFactory
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util.DefaultPayload
import Server._

object FireAndForget {

  def main(args: Array[String]): Unit = {
    val socket = RSocketFactory.connect
      .transport(TcpClientTransport
        .create(HOST, PORT)).start.block

    socket.fireAndForget(DefaultPayload.create("Hello world!")).block
  }

}
