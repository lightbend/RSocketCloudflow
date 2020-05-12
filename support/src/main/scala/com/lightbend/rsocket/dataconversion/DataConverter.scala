package com.lightbend.rsocket.dataconversion

import java.nio.ByteBuffer

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase

class DataConverter[T <: SpecificRecordBase](avroSchema: Schema) extends Serializable {

  val recordInjection: Injection[T, Array[Byte]] = SpecificAvroCodecs.toBinary(avroSchema)
  val avroSerde = new AvroSerde(recordInjection)

  def toBytes(data: T): Array[Byte] = avroSerde.encode(data)

  def fromBytes(data: Array[Byte]): Option[T] = {
    try {
      Some(avroSerde.decode(data))
    } catch {
      case t: Throwable ⇒
        t.printStackTrace()
        None
    }
  }

  def fromByteBuffer(buffer: ByteBuffer): Option[T] = {
    val data = new Array[Byte](buffer.remaining())
    buffer.get(data)
    fromBytes(data)
  }
}
