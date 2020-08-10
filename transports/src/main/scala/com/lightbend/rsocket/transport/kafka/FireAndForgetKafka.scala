package com.lightbend.rsocket.transport.kafka

import com.lightbend.rsocket.transport.kafka.embedded.KafkaEmbedded
import io.rsocket._
import io.rsocket.core._
import io.rsocket.util.ByteBufPayload
import reactor.core.publisher.Mono

object FireAndForgetKafka {

  private val BOOTSTRAP_SERVERS = "localhost:9092"
  private val TOPIC = "boris"

  def main(args: Array[String]): Unit = {

    // Create Kafka
    val kafka = new KafkaEmbedded

    // Create server
    val server = RSocketServer.create(SocketAcceptor.forFireAndForget((payload: Payload) => {
      // Log message
      println(s"Received 'fire-and-forget' request with payload: [${payload.getDataUtf8}]")
      Mono.empty()
    }))
      .bind(KafkaServerTransport.create(BOOTSTRAP_SERVERS, TOPIC))
      .subscribe

    // Create client
    val client = RSocketConnector.create()
      .connect(KafkaClientTransport.create(BOOTSTRAP_SERVERS, TOPIC))
      .block()

    // Send some messages
    val n = 20
    1 to n foreach { i =>
      client.fireAndForget(ByteBufPayload.create("message " + i)).block
      Thread.sleep(100);
    }
    // Wait and complete
    Thread.sleep(10000)
    client.dispose();
    server.dispose()
    System.exit(0)
  }
}