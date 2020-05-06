package com.lightbend.sensordata.support

import java.nio.ByteBuffer

import cloudflow.examples.sensordata.rsocket.avro._
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs

object SensorDataConverter {
  import AvroSerializer._
  implicit private val serializer: AvroSerializer[SensorData] = new SensorDataSerializer

  def toBytes(data: SensorData): Array[Byte] = data
  def fromByteBuffer(buffer: ByteBuffer): Option[SensorData] = buffer
}

class SensorDataSerializer extends AvroSerializer[SensorData] {
  private val sensorRecordInjection: Injection[SensorData, Array[Byte]] = SpecificAvroCodecs.toBinary(SensorData.SCHEMA$)
  private val sensorAvroSerde: AvroSerde[SensorData] = new AvroSerde(sensorRecordInjection)

  private def fromBytes(data: Array[Byte]): Option[SensorData] = {
    try {
      Some(sensorAvroSerde.decode(data))
    } catch {
      case t: Throwable ⇒
        t.printStackTrace()
        None
    }
  }
  def apply(buffer: ByteBuffer): Option[SensorData] = {
    val data = new Array[Byte](buffer.remaining())
    buffer.get(data)
    fromBytes(data)
  }

  def apply(data: SensorData): Array[Byte] = sensorAvroSerde.encode(data)
}
