package com.lightbend.sensordata

import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import cloudflow.examples.sensordata.rsocket.avro._

class InvalidMetricLogger extends AkkaStreamlet {
  val inlet = AvroInlet[InvalidMetric]("in")
  val shape = StreamletShape.withInlets(inlet)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph = {
      sourceWithCommittableContext(inlet).map(m ⇒ println(s"Invalid metric detected! $m")).to(committableSink)
    }
  }
}
