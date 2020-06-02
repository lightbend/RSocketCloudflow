package com.lightbend.rsocket.examples

import java.util

import io.rsocket._
import io.rsocket.core._
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.client.filter.RSocketSupplier
import io.rsocket.client.LoadBalancedRSocketMono
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.util.ByteBufPayload
import org.slf4j.LoggerFactory
import reactor.core.publisher._

import scala.collection.JavaConverters._

object Multiserver {

  private val logger = LoggerFactory.getLogger(this.getClass) // Logger
  private val ports = List(7000, 7001, 7002) // Ports
  private var hits = Map(7000 -> 0, 7001 -> 0, 7002 -> 0) // Stats map

  def main(args: Array[String]): Unit = {

    // Create servers
    ports.foreach(port =>
      RSocketServer.create(SocketAcceptor.forFireAndForget((payload: Payload) => {
        // Peg request to the server
        addHit(port)
        payload.release()
        Mono.empty()
      }))
        // Enable Zero Copy
        .payloadDecoder(PayloadDecoder.ZERO_COPY)
        .bind(TcpServerTransport.create("0.0.0.0", port))
        .block)

    // Create socket suppliers (clients)
    val rsocketSuppliers = ports.map(port =>
      new RSocketSupplier(() => Mono.just(RSocketConnector.create()
        .payloadDecoder(PayloadDecoder.ZERO_COPY)
        .connect(TcpClientTransport.create("0.0.0.0", port)).block()))).asJava

    // Create load balancer
    val balancer = LoadBalancedRSocketMono.create(Flux.create(
      (sink: FluxSink[util.Collection[RSocketSupplier]]) => sink.next(rsocketSuppliers)))

    // Send messages
    1 to 300 foreach { _ =>
      balancer.doOnNext(
        socket => socket.fireAndForget(ByteBufPayload.create("Hello world")).block()).block()
    }

    // Wait to make sure that process completes
    Thread.sleep(3000)

    // Print execution statistics
    println("Execution statistics")
    hits.foreach(entry => println(s"Port : ${entry._1} -> count ${entry._2}"))
  }

  // Collect statistics
  def addHit(port: Int): Unit = this.synchronized { hits += port -> (hits(port) + 1) }
}