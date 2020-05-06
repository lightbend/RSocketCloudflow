package com.lightbend.sensordata

import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import cloudflow.examples.sensordata.rsocket.avro._

class RotorSpeedFilter extends AkkaStreamlet {
  val in = AvroInlet[Metric]("in")
  val out = AvroOutlet[Metric]("out")
  val shape = StreamletShape(in, out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph = sourceWithOffsetContext(in).via(flow).to(committableSink(out))
    def flow = FlowWithCommittableContext[Metric].filter(_.name == "rotorSpeed")
  }
}
