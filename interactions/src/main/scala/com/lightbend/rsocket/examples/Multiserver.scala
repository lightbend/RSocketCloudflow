package com.lightbend.rsocket.examples

import java.util

import io.rsocket._
import io.rsocket.core._
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.client.filter.RSocketSupplier
import io.rsocket.client.LoadBalancedRSocketMono
import io.rsocket.util.DefaultPayload
import org.slf4j.LoggerFactory
import reactor.core.publisher._

import scala.collection.JavaConverters._


object Multiserver {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val ports = List(7000, 7001, 7002)
  private var hits = Map(7000 -> 0, 7001 -> 0, 7002 -> 0)

  def main(args: Array[String]): Unit = {
    ports.foreach(port =>
      RSocketServer.create(new FFAcceptorImplementation(port))
        .bind(TcpServerTransport.create("0.0.0.0", port))
        .block
    )

    val rsocketSuppliers = ports.map(port =>
      new RSocketSupplier(() => Mono.just(RSocketConnector
        .connectWith(TcpClientTransport.create("0.0.0.0", port)).block()))
    ).asJava

    val balancer = LoadBalancedRSocketMono.create(Flux.create(
      (sink: FluxSink[util.Collection[RSocketSupplier]]) => sink.next(rsocketSuppliers)
    ))


    System.out.println(s"Created balancer : $balancer")

    1 to 300 foreach{_ =>
      balancer.doOnNext(
        socket => socket.fireAndForget(DefaultPayload.create("Hello world")).block()
      ).block()
    }

    Thread.sleep(2000)
    println("Execution statistics")
    hits.foreach(entry => println(s"Port : ${entry._1} -> count ${entry._2}"))
  }

  def addHit(port: Int) : Unit = hits += (port -> (hits(port) + 1))
}

class FFAcceptorImplementation(port : Int) extends SocketAcceptor {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
/*
        try {
          logger.info(s"Received 'fire-and-forget' request with payload: [${payload.getDataUtf8}] on port $port")
        } catch {
          case t: Throwable =>
            println(s"Exception ${t.getMessage} on on port $port")
        } */
        Multiserver.addHit(port)
        Mono.empty()
      }
    })
}