package com.lightbend.sensordata.rsocket.ingress

import java.util.concurrent.LinkedBlockingDeque

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
    akka.stream.scaladsl.Source.fromGraph(new RSocketSource(containerPort, acceptor))
      .map(dataConverter.fromBytes)
      .collect { case (Some(data)) ⇒ data }
      .runWith(plainSink(outlet))
    ()
  }
}

class RSocketSourceAcceptorImpl extends RSocketSourceAcceptor {

  private val blockingDeque = new LinkedBlockingDeque[Array[Byte]]()

  override def nextSensorData(): Array[Byte] = blockingDeque.take()

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new RSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        // Get data
        val buffer = payload.getData
        val data = new Array[Byte](buffer.remaining())
        buffer.get(data)
        // Queue it for processing
        blockingDeque.add(data)
        Mono.empty()
      }
    })
}
