package com.lightbend.sensordata.support

import java.nio.ByteBuffer

object AvroSerializer {
  implicit def avroable[A: AvroSerializer](x: A): Array[Byte] =
    implicitly[AvroSerializer[A]] apply x
  implicit def avroable[T: AvroSerializer](x: ByteBuffer): Option[T] =
    implicitly[AvroSerializer[T]] apply x
}

abstract class AvroSerializer[T] {
  def apply(buffer: ByteBuffer): Option[T]
  def apply(d: T): Array[Byte]
}

