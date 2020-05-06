package com.lightbend.sensordata.support

import java.nio.ByteBuffer

import cloudflow.examples.sensordata.rsocket.avro._
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs

object SensorDataConverter {

  private val sensorRecordInjection: Injection[SensorData, Array[Byte]] = SpecificAvroCodecs.toBinary(SensorData.SCHEMA$)
  private val sensorAvroSerde: AvroSerde[SensorData] = new AvroSerde(sensorRecordInjection)

  def toBytes(data: SensorData): Array[Byte] = sensorAvroSerde.encode(data)

  def fromBytes(data: Array[Byte]): Option[SensorData] = {
    try {
      Some(sensorAvroSerde.decode(data))
    } catch {
      case t: Throwable ⇒
        t.printStackTrace()
        None
    }
  }

  def fromByteBuffer(buffer: ByteBuffer): Option[SensorData] = {
    val data = new Array[Byte](buffer.remaining())
    buffer.get(data)
    fromBytes(data)
  }
}
