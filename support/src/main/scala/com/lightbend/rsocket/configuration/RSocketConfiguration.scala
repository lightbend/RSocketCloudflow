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
    config.getInt("rsocket.port")
  } catch {
    case _: Throwable ⇒ 3000
  }
  // Producer option
  val PRODUCER_OPTION = try {
    val p = config.getInt("producer.option")
    if((p < 1) || (p > 3))
      throw new Exception(s"Producer option $p is not valid, it has to be between 1 and 3")
    p
  } catch {
    case _: Throwable ⇒ 1
  }
}
