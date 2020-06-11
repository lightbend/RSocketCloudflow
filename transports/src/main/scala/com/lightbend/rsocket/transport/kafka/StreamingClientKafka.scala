package com.lightbend.rsocket.transport.kafka

import com.lightbend.rsocket.transport.kafka.embedded.KafkaEmbedded
import io.rsocket._
import io.rsocket.core._
import io.rsocket.util._
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import reactor.core.publisher._

object StreamingClientKafka {

  private val BOOTSTRAP_SERVERS = "localhost:9092"
  private val TOPIC = "boris"

  def main(args: Array[String]): Unit = {

    // Create Kafka
    val kafka = new KafkaEmbedded

    // Ensure clean disposal
    Hooks.onErrorDropped((t: Throwable) => {})

    // Create a server
    RSocketServer.create(SocketAcceptor.forRequestStream((payload: Payload) => {
      // Log request
      println(s"Server Received 'request stream' request with payload: [${payload.getDataUtf8}] ")
      // return stream
      Flux.generate[Payload, Int](() => 0, (state: Int, sink: SynchronousSink[Payload]) => {
        Thread.sleep(100)
        sink.next(DefaultPayload.create("Interval: " + state))
        state + 1
      })
    }))
      .bind(KafkaServerTransport.create(BOOTSTRAP_SERVERS,TOPIC))

    // create a client
    val client = RSocketConnector.create()
      .connect(KafkaClientTransport.create(BOOTSTRAP_SERVERS,TOPIC))
      .block

    // Send messages
    client
      .requestStream(DefaultPayload.create("Hello"))
      .doOnNext((payload: Payload) => {
        println(s"Client Received next stream element with payload: [${payload.getDataUtf8}] ")
      })
      .`then`().block();
/*
      .subscribe(new BaseSubscriber[Payload] {
        // Back pressure subscriber
        private val log = LoggerFactory.getLogger(this.getClass)
        val NUMBER_OF_REQUESTS_TO_PROCESS = 5l
        var receivedItems = 0
        // Start subscription
        override def hookOnSubscribe(subscription: Subscription): Unit = {
          subscription.request(NUMBER_OF_REQUESTS_TO_PROCESS)
        }
        // Processing request
        override def hookOnNext(value: Payload): Unit = {
          log.info(s"New stream element ${value.getDataUtf8}")
          receivedItems += 1
          if (receivedItems % NUMBER_OF_REQUESTS_TO_PROCESS == 0) {
            log.info(s"Requesting next [$NUMBER_OF_REQUESTS_TO_PROCESS] elements")
            request(NUMBER_OF_REQUESTS_TO_PROCESS)
          }
        }
        // Invoked on stream completion
        override def hookOnComplete(): Unit = log.info("Completing subscription")
        // Invoked on stream error
        override def hookOnError(throwable: Throwable): Unit = log.error(s"Stream subscription error [$throwable]")
        // Invoked on stream cancelation
        override def hookOnCancel(): Unit = log.info("Subscription canceled")
      }) */

    // Wait for completion
    Thread.sleep(10000)

    client.dispose();
  }
}
