package com.lightbend.rsocket.dataconversion

import cloudflow.streamlets.DecodeException
import com.twitter.bijection.Injection
import org.apache.avro.specific.SpecificRecordBase

import scala.util.{ Failure, Try }

class AvroSerde[T <: SpecificRecordBase](injection: Injection[T, Array[Byte]]) extends Serializable {
  val inverted: Array[Byte] ⇒ Try[T] = injection.invert _

  def encode(value: T): Array[Byte] = injection(value)

  def decode(bytes: Array[Byte]): T = Try(inverted(bytes).get).recoverWith {
    case t ⇒
      Failure(DecodeException("Could not decode.", t))
  }.get
}
