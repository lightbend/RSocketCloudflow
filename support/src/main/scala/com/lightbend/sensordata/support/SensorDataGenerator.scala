package com.lightbend.sensordata.support

import java.time.Instant
import java.util.UUID

import cloudflow.examples.sensordata.rsocket.avro.{ Measurements, SensorData }

import scala.util.Random

object SensorDataGenerator {
  private val randomGenerator = new Random() // Random generator

  def random(): SensorData = new SensorData(UUID.randomUUID(), Instant.ofEpochMilli(System.currentTimeMillis()),
    new Measurements(randomGenerator.nextInt(1000) / 10.0, randomGenerator.nextInt(20000) / 100.0, randomGenerator.nextInt(2000) / 10.0))

  // Create a JSON message
  def randomJsonString: String =
    s"""{
       |        "deviceId": "${UUID.randomUUID().toString}",
       |        "timestamp": ${System.currentTimeMillis()},
       |        "measurements": {
       |            "power":  ${randomGenerator.nextInt(1000) / 10.0},
       |            "rotorSpeed": ${randomGenerator.nextInt(20000) / 100.0},
       |            "windSpeed": ${randomGenerator.nextInt(2000) / 10.0}
       |         }
       |     }""".stripMargin
}
