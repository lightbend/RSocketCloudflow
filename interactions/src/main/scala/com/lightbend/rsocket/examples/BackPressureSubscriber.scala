package com.lightbend.rsocket.examples

import io.rsocket.Payload
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import reactor.core.publisher.BaseSubscriber

object BackPressureSubscriber {
  val NUMBER_OF_REQUESTS_TO_PROCESS = 5
}

class BackPressureSubscriber extends BaseSubscriber[Payload] {

  private val log = LoggerFactory.getLogger(this.getClass)
  var receivedItems = 0

  import BackPressureSubscriber._

  override def hookOnSubscribe(subscription: Subscription): Unit = subscription.request(NUMBER_OF_REQUESTS_TO_PROCESS)

  override def hookOnNext(value: Payload): Unit = {
    receivedItems += 1
    if (receivedItems % NUMBER_OF_REQUESTS_TO_PROCESS == 0) {
      log.info(s"Requesting next [$NUMBER_OF_REQUESTS_TO_PROCESS] elements")
      request(NUMBER_OF_REQUESTS_TO_PROCESS)
    }
  }

  override def hookOnComplete(): Unit = log.info("Completing subscription")

  override def hookOnError(throwable: Throwable): Unit = log.error(s"Stream subscription error [$throwable]")
}
