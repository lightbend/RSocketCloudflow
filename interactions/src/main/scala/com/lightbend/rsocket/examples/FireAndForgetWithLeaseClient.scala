package com.lightbend.rsocket.examples

import java.time.Duration
import java.util._
import java.util.concurrent.LinkedBlockingDeque
import java.util.function._

import io.rsocket._
import io.rsocket.core._
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.lease._
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.TcpServerTransport
import io.rsocket.util.ByteBufPayload
import reactor.core.publisher._
import reactor.util.retry.Retry


object FireAndForgetWithLeaseClient {

  private val blockingQueue = new LinkedBlockingDeque[String]()
  private val SERVER_TAG = "server"
  private val CLIENT_TAG = "client"

  def main(args: Array[String]): Unit = {

    // Create server
    val server = RSocketServer.create((setup: ConnectionSetupPayload, sendingSocket: RSocket) => {
      Mono.just(new RSocket() {
        override def fireAndForget(payload: Payload): Mono[Void] = {
          // Log message
          blockingQueue.add(payload.getDataUtf8)
          payload.release()
          Mono.empty()
        }})})
      .lease(() => Leases.create().sender(new LeaseCalculator(SERVER_TAG, blockingQueue)))
      // Enable Zero Copy
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bindNow(TcpServerTransport.create("0.0.0.0", 7000))

    // Lease notification reciever
    val receiver = new LeaseReceiver(CLIENT_TAG)

    // Create client
    val clientRSocket = RSocketConnector.create
      .lease(() => Leases.create.receiver(receiver))
      // Enable Zero Copy
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .connect(TcpClientTransport.create(server.address)).block

    // Create queue drainer
    new Thread(new QueueDrainer(blockingQueue)).start()

    // Send messages
    Flux.generate(() => 0L, (state: Long, sink: SynchronousSink[Long]) => {
        sink.next(state)
        state + 1
    })
      // Wait for the  Lease arrival
      .delaySubscription(receiver.notifyWhenNewLease().then())
      .concatMap((tick : Long) => {
          println(s"Sending $tick")
          clientRSocket
            .fireAndForget(ByteBufPayload.create(s"New tick = $tick"))
            .retryWhen(
              Retry.indefinitely()
                .filter((t : Throwable) => t.isInstanceOf[MissingLeaseException])
                .doBeforeRetryAsync(rs => {
                  System.out.println("Ran out of leases")
                  receiver.notifyWhenNewLease().then()
                }))
        }
      )
      .blockLast();

    // Wait and complete
    Thread.sleep(2000)
    clientRSocket.onClose.block
    server.dispose()
  }
}

class QueueDrainer(queue : LinkedBlockingDeque[String]) extends Runnable{
  override def run(): Unit = {
    while(true)
      queue.take()
//      println(s"New ff message ${queue.take()}")
  }
}


// This is a class responsible for making decision on whether server is ready to
// receive new FireAndForget or not base in the number of messages enqueued
class LeaseCalculator(tag : String, queue : LinkedBlockingDeque[String]) extends Function[Optional[LeaseStats], Flux[Lease]] {

  val leaseDuration = Duration.ofSeconds(1)
  val maxQueueDepth = 50

  override def apply(leaseStats: Optional[LeaseStats]): Flux[Lease] = {
    val stats = leaseStats.isPresent() match {
      case true => "present"
      case _ => "absent"
    }
    println(s"$tag stats are $stats")

    Flux.interval(leaseDuration)
      .handle((_, sink : SynchronousSink[Lease]) => {
        (maxQueueDepth - queue.size()) match {
          case requests if (requests > 0) => sink.next(Lease.create(leaseDuration.toMillis.toInt, requests))
          case _ =>
        }
      })
  }
}

// Lease reciever handler
class LeaseReceiver(tag : String) extends Consumer[Flux[Lease]] {

  val direct : DirectProcessor[Lease] = DirectProcessor.create()

  override def accept(receivedLeases: Flux[Lease]): Unit = {
    receivedLeases
      .subscribe((l:Lease) => {
          direct.onNext(l);
          println(s"$tag received leases - ttl: ${l.getTimeToLiveMillis()}, requests: ${l.getAllowedRequests()}")
        });
  }

  def notifyWhenNewLease(): Mono[Lease] = direct.next
}