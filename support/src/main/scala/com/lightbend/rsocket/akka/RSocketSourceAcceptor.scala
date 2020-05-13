package com.lightbend.rsocket.akka

import io.rsocket.SocketAcceptor

// Extension to RSocket acceptor, providing additional methods for accessing data
trait RSocketSourceAcceptor extends SocketAcceptor {
  def nextSensorData(): Array[Byte]
}
