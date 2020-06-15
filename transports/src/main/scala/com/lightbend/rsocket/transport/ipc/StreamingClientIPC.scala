package com.lightbend.rsocket.transport.ipc

import com.lightbend.rsocket.transport.kafka.embedded.KafkaEmbedded
import io.rsocket._
import io.rsocket.core._
import io.rsocket.util._
import org.reactivestreams.Subscription
import reactor.core.publisher._

object StreamingClientIPC {

  private val directory = "tmp/boris"

  def main(args: Array[String]): Unit = {


    // Create a server
    RSocketServer.create(SocketAcceptor.forRequestStream((payload: Payload) => {
      // Log request
      println(s"Server Received request stream request with payload: [${payload.getDataUtf8}] ")
      // return stream
      Flux.generate[Payload, Int](() => 0, (state: Int, sink: SynchronousSink[Payload]) => {
        Thread.sleep(100)
        sink.next(DefaultPayload.create("Interval: " + state))
        state + 1
      })
    }))
      .bind(IPCServerTransport.create(directory))
      .subscribe()

    // create a client
    var client = RSocketConnector.create()
      .connect(IPCClientTransport.create(directory))
      .block

    // Send messages
    client
      .requestStream(DefaultPayload.create("Hello"))
      .subscribe(new BaseSubscriber[Payload] {
        // Back pressure subscriber
        val NUMBER_OF_REQUESTS_TO_PROCESS = 5l
        var receivedItems = 0

        // Start subscription
        override def hookOnSubscribe(subscription: Subscription): Unit = {
          subscription.request(Long.MaxValue)
        }

        // Processing request
        override def hookOnNext(value: Payload): Unit = {
          println(s"New stream element ${value.getDataUtf8}")
          receivedItems += 1
          if (receivedItems % NUMBER_OF_REQUESTS_TO_PROCESS == 0) {
            println(s"Requesting next [$NUMBER_OF_REQUESTS_TO_PROCESS] elements")
            request(NUMBER_OF_REQUESTS_TO_PROCESS)
          }
        }

        // Invoked on stream completion
        override def hookOnComplete(): Unit = println("Completing subscription")

        // Invoked on stream error
        override def hookOnError(throwable: Throwable): Unit = println(s"Stream subscription error [$throwable]")

        // Invoked on stream cancelation
        override def hookOnCancel(): Unit = println("Subscription canceled")
      })

    // Wait for completion
    Thread.sleep(5000)

    client.dispose()
  }
}