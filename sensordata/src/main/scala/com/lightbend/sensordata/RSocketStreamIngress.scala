package com.lightbend.sensordata

import java.nio.ByteBuffer

import cloudflow.akkastream._
import cloudflow.examples.sensordata.rsocket.avro.SensorData
import cloudflow.streamlets.{CodecOutlet, StreamletShape}
import cloudflow.streamlets.avro.AvroOutlet
import com.lightbend.sensordata.support.DataConverter
import io.rsocket._
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server._
import io.rsocket.util.DefaultPayload
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import reactor.core.publisher._

class RSocketStreamIngress extends AkkaServerStreamlet {

  val out = AvroOutlet[SensorData]("out")

  def shape = StreamletShape.withOutlets(out)

  override def createLogic() = new RSocketStreamRequestLogic(this, SensorData.SCHEMA$, out)
}

class RSocketStreamRequestLogic[out <: SpecificRecordBase](server: Server, schema: Schema, outlet: CodecOutlet[out])
  (implicit context: AkkaStreamletContext) extends ServerStreamletLogic(server) {

  override def run(): Unit = {
    RSocketServer
      .create(new RSocketBinaryStreamAcceptorImpl(sinkRef(outlet), schema))
      .bind(WebsocketServerTransport.create("0.0.0.0", containerPort))
      .block()
    println(s"Bound RSocket server to port $containerPort")
  }
}

class RSocketBinaryStreamAcceptorImpl[out <: SpecificRecordBase](writer: WritableSinkRef[out], schema: Schema) extends SocketAcceptor {

  val dataConverter = new DataConverter[out](schema)

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] = {
    reactiveSocket
      .requestStream(DefaultPayload.create("Please may I have a stream"))
      .map[ByteBuffer](payload ⇒ payload.getData)
      .doOnNext(data ⇒ {
        dataConverter.fromByteBuffer(data).map(writer.write)
        ()
      })
      .subscribe()
    Mono.empty()
  }
}