package com.lightbend.rsocket.akka

import java.util.concurrent.LinkedBlockingDeque

import akka.stream._
import akka.stream.stage._
import io.rsocket._
import io.rsocket.core.RSocketServer
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.transport.netty.server.TcpServerTransport
import reactor.core.publisher.Mono

class RSocketSource(port: Int, host: String = "0.0.0.0") extends GraphStage[SourceShape[Array[Byte]]] {

  // the outlet port of this stage which produces Ints
  val out: akka.stream.Outlet[Array[Byte]] = Outlet("RsocketSourceOut")
  // Coordination queue
  private val blockingQueue = new LinkedBlockingDeque[Array[Byte]]()

  override val shape: SourceShape[Array[Byte]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    // Pre start. create server
    override def preStart(): Unit = {
      RSocketServer.create(SocketAcceptor.forFireAndForget((payload: Payload) ⇒ {
        // Get data
        val buffer = payload.getData
        val data = new Array[Byte](buffer.remaining())
        buffer.get(data)
        // Queue it for processing
        blockingQueue.add(data)
        payload.release()
        Mono.empty()
      }))
        .payloadDecoder(PayloadDecoder.ZERO_COPY)
        .bind(TcpServerTransport.create(host, port))
        .subscribe
      println(s"Bound RSocket server to $host:$port")
    }

    // create a handler for the outlet
    setHandler(out, new OutHandler {
      // when you are asked for data (pulled for data)
      override def onPull(): Unit = {
        // emit an element on the outlet, if exists
        push(out, blockingQueue.take())
      }
    })
  }
}
