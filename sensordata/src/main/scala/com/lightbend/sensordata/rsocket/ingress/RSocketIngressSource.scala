package com.lightbend.sensordata.rsocket.ingress

import cloudflow.akkastream._
import cloudflow.examples.sensordata.rsocket.avro._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import com.lightbend.rsocket.akka._
import com.lightbend.rsocket.dataconversion.DataConverter
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase

class RSocketIngressSource extends AkkaServerStreamlet {

  val out: AvroOutlet[SensorData] = AvroOutlet[SensorData]("out").withPartitioner(RoundRobinPartitioner)

  def shape: StreamletShape = StreamletShape.withOutlets(out)

  // Create Logic
  override def createLogic() = new RSocketSourceStreamletLogic(this, SensorData.SCHEMA$, out)
}

// Create logic
class RSocketSourceStreamletLogic[out <: SpecificRecordBase]
  (server: cloudflow.akkastream.Server, schema: Schema, outlet: CodecOutlet[out])
  (implicit context: AkkaStreamletContext) extends ServerStreamletLogic(server) {

  val dataConverter = new DataConverter[out](schema) // Marshaller

  override def run(): Unit = {

    // Process flow
    akka.stream.scaladsl.Source.fromGraph(new RSocketSource(containerPort))
      .map(dataConverter.fromBytes)
      .collect { case (Some(data)) ⇒ data }
      .runWith(plainSink(outlet))
    ()
  }
}
