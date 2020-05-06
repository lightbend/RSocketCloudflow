package com.lightbend.sensordata

import cloudflow.akkastream.{ Server, _ }
import cloudflow.examples.sensordata.rsocket.avro._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import com.lightbend.sensordata.support.DataConverter
import io.rsocket._
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server.TcpServerTransport
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.Schema
import reactor.core.publisher._

class RSocketBinaryIngress extends AkkaServerStreamlet {

  val out = AvroOutlet[SensorData]("out")

  def shape = StreamletShape.withOutlets(out)

  override def createLogic() = new RSocketBinaryStreamletLogic(this, SensorData.SCHEMA$, out)
}

class RSocketBinaryStreamletLogic[out <: SpecificRecordBase](server: Server, schema: Schema, outlet: CodecOutlet[out])
  (implicit context: AkkaStreamletContext) extends ServerStreamletLogic(server) {

  override def run(): Unit = {
    RSocketServer.create(new RSocketBinaryAcceptorImpl(sinkRef(outlet), schema))
      .bind(TcpServerTransport.create("0.0.0.0", containerPort))
      .subscribe
    println(s"Bound RSocket server to port $containerPort")
  }
}

class RSocketBinaryAcceptorImpl[out <: SpecificRecordBase](writer: WritableSinkRef[out], schema: Schema) extends SocketAcceptor {

  val dataConverter = new DataConverter[out](schema)

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        dataConverter.fromByteBuffer(payload.getData).map(writer.write)
        Mono.empty()
      }
    })
}
