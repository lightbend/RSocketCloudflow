package com.lightbend.rsocket.akka

import akka.stream._
import akka.stream.stage._
import io.rsocket.core.RSocketServer
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.transport.netty.server.TcpServerTransport

class RSocketSource(port: Int, acceptor: RSocketSourceAcceptor) extends GraphStage[SourceShape[Array[Byte]]] {

  // the outlet port of this stage which produces Ints
  val out: akka.stream.Outlet[Array[Byte]] = Outlet("RsocketSourceOut")

  override val shape: SourceShape[Array[Byte]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    // Pre start. create server
    override def preStart(): Unit = {
      RSocketServer.create(acceptor)
        .payloadDecoder(PayloadDecoder.ZERO_COPY)
        .bind(TcpServerTransport.create("0.0.0.0", port))
        .subscribe
      println(s"Bound RSocket server to port $port")
    }

    // create a handler for the outlet
    setHandler(out, new OutHandler {
      // when you are asked for data (pulled for data)
      override def onPull(): Unit = {
        // emit an element on the outlet, if exists
        push(out, acceptor.nextSensorData())
      }
    })
  }
}
