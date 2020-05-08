package com.lightbend.sensordata

import akka.stream._
import akka.stream.stage._
import cloudflow.akkastream._
import cloudflow.examples.sensordata.rsocket.avro._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import com.lightbend.sensordata.support.DataConverter
import io.rsocket._
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server.TcpServerTransport
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import reactor.core.publisher._

import scala.collection.mutable._

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
    akka.stream.scaladsl.Source.fromGraph(new RSocketSource(acceptor)).collect { case (Some(data)) ⇒ data }
      .map(dataConverter.fromBytes(_)).collect { case (Some(data)) ⇒ data }.runWith(plainSink(outlet))
    // Start server
    RSocketServer.create(acceptor)
      .bind(TcpServerTransport.create("0.0.0.0", containerPort))
      .subscribe
    println(s"Bound RSocket server to port $containerPort")

  }
}

class RSocketSource(acceptor: RSocketSourceAcceptorImpl) extends GraphStage[SourceShape[Option[Array[Byte]]]] {

  // the outlet port of this stage which produces Ints
  val out: akka.stream.Outlet[Option[Array[Byte]]] = Outlet("RsocketSource")

  override val shape: SourceShape[Option[Array[Byte]]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    // create a handler for the outlet
    setHandler(out, new OutHandler {
      // when you are asked for data (pulled for data)
      override def onPull(): Unit = {
        // emit an element on the outlet, if exists
        push(out, acceptor.nextSensorData())
      }
    })
  }
}

class RSocketSourceAcceptorImpl extends SocketAcceptor {

  private val data = new Queue[Array[Byte]]()

  def nextSensorData(): Option[Array[Byte]] = this.synchronized {
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
