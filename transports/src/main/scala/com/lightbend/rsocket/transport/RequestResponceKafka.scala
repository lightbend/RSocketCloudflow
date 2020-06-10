package com.lightbend.rsocket.transport

import com.lightbend.rsocket.transport.kafka._
import com.lightbend.rsocket.transport.kafka.embedded.KafkaEmbedded
import io.rsocket._
import io.rsocket.core._
import io.rsocket.util.DefaultPayload
import reactor.core.publisher.Mono


object RequestResponceKafka {

  private val BOOTSTRAP_SERVERS = "localhost:9092"
  private val TOPIC = "boris"


  def main(args: Array[String]): Unit = {

    // Create Kafka
    val kafka = new KafkaEmbedded

    // Server
    val server = RSocketServer.create(SocketAcceptor.forRequestResponse((payload: Payload) => {
      // Just return payload back
      println(s"Server got request ${payload.getDataUtf8}")
      Mono.just(payload)
    }))
      .bind(KafkaServerTransport.create(BOOTSTRAP_SERVERS,TOPIC))
      .subscribe

    // Client
    val client = RSocketConnector.create()
      .connect(KafkaClientTransport.create(BOOTSTRAP_SERVERS,TOPIC))
      .block

    val n = 20
    1 to n foreach  {i =>
      client.requestResponse(DefaultPayload.create("message " + i))
        .map((payload: Payload) => {
          println(s"Client got reply ${payload.getDataUtf8}")
          payload
        })
        .block
    }
    client.dispose()
    server.dispose()
    System.exit(0)
  }
}