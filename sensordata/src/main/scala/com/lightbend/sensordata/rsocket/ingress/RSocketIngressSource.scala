package com.lightbend.sensordata.rsocket.ingress

import cloudflow.akkastream._
import cloudflow.examples.sensordata.rsocket.avro._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import io.rsocket._
import com.lightbend.rsocket.akka._
import com.lightbend.rsocket.dataconversion.DataConverter
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import reactor.core.publisher._

import scala.collection.mutable._

//TODO Remove before going public
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

  val acceptor = new RSocketSourceAcceptorImpl() // Acceptor
  val dataConverter = new DataConverter[out](schema) // Marshaller

  override def run(): Unit = {

    // Process flow
    akka.stream.scaladsl.Source.fromGraph(new RSocketSource(containerPort, acceptor)).collect { case (Some(data)) ⇒ data }
      .map(dataConverter.fromBytes(_)).collect { case (Some(data)) ⇒ data }.runWith(plainSink(outlet))
    ()
  }
}

class RSocketSourceAcceptorImpl extends RSocketSourceAcceptor {

  private val data = new Queue[Array[Byte]]()

  override def nextSensorData(): Option[Array[Byte]] = this.synchronized {
    data.isEmpty match {
      case true ⇒ None
      case _    ⇒ Some(data.dequeue())
    }
  }

  def addSensorData(sensor: Array[Byte]): Unit = this.synchronized {
    data += sensor
    ()
  }

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        // Get data
        val buffer = payload.getData
        val data = new Array[Byte](buffer.remaining())
        buffer.get(data)
        // Queue it for processing
        addSensorData(data)
        Mono.empty()
      }
    })
}
