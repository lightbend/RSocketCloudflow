package com.lightbend.rsocket.configuration

import com.typesafe.config.ConfigFactory

/**
 * Various configuration parameters.
 */

object RSocketConfiguration {

  val config = ConfigFactory.load()

  // Rsocket host
  val RSOCKET_HOST = try {
    config.getString("rsocket.host")
  } catch {
    case _: Throwable ⇒ "0.0.0.0"
  }
  // Rsocket port
  val RSOCKET_PORT = try {
    config.getString("rsocket.port").toInt
  } catch {
    case _: Throwable ⇒ 3000
  }
  // Producer option
  val PRODUCER_OPTION = try {
    val p = config.getString("producer.option").toInt
    if ((p < 1) || (p > 3))
      throw new Exception(s"Producer option $p is not valid, it has to be between 1 and 3")
    p
  } catch {
    case _: Throwable ⇒ 1
  }
  // Producer interval
  val PRODUCER_INTERVAL = try { // in ms
    config.getString("producer.interval").toLong
  } catch {
    case _: Throwable ⇒ 1000l
  }
}
