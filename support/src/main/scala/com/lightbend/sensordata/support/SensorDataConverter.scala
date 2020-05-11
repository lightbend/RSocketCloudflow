package com.lightbend.sensordata.support

import java.nio.ByteBuffer

import cloudflow.examples.sensordata.rsocket.avro.SensorData

object SensorDataConverter {
  private val converter = new DataConverter[SensorData](SensorData.SCHEMA$)
  def toBytes(data: SensorData): Array[Byte] = converter.toBytes(data)
  def apply(buffer: ByteBuffer): Option[SensorData] = converter.fromByteBuffer(buffer)

}
