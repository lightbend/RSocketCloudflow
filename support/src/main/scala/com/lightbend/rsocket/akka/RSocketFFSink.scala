package com.lightbend.rsocket.akka

import akka.stream._
import akka.stream.stage._
import io.rsocket._
import io.rsocket.core._
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.util._

class RSocketFFSink(port: Int, host: String = "0.0.0.0") extends GraphStage[SinkShape[Array[Byte]]] {
  // the input port of the Sink which consumes Ints
  val in: Inlet[Array[Byte]] = Inlet("RsocketSinkInput")
  var socket: RSocket = null

  override def shape: SinkShape[Array[Byte]] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      // ask for an element as soon as you start
      override def preStart(): Unit = {
        socket = RSocketConnector
          .connectWith(TcpClientTransport.create(host, port))
          .block
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          // grab the value from the buffer
          val payload = grab(in)
          // do operation
          socket.fireAndForget(DefaultPayload.create(payload)).block
          // ask for another
          pull(in)
        }
      })
    }
}
