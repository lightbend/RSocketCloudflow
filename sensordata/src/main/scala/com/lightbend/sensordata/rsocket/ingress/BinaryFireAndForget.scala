package com.lightbend.sensordata.rsocket.ingress

import cloudflow.akkastream.{ Server, _ }
import cloudflow.examples.sensordata.rsocket.avro._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import com.lightbend.rsocket.dataconversion.SensorDataConverter
import io.rsocket._
import io.rsocket.core.RSocketServer
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.transport.netty.server.TcpServerTransport
import reactor.core.publisher._

class BinaryFireAndForget extends AkkaServerStreamlet {

  val out = AvroOutlet[SensorData]("out")

  def shape = StreamletShape.withOutlets(out)

  override def createLogic() = new BinaryFireAndForgetStreamletLogic(this, out)
}

// Create logic
class BinaryFireAndForgetStreamletLogic(server: Server, outlet: CodecOutlet[SensorData])
  (implicit context: AkkaStreamletContext) extends ServerStreamletLogic(server) {

  override def run(): Unit = {
    // Create server
    RSocketServer.create(new BinaryFireAndForgetAcceptor(sinkRef(outlet)))
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(TcpServerTransport.create("0.0.0.0", containerPort))
      .subscribe
    println(s"Bound RSocket server to port $containerPort")
  }
}

class BinaryFireAndForgetAcceptor(writer: WritableSinkRef[SensorData]) extends SocketAcceptor {

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new RSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        // Get message and write to sink
        SensorDataConverter(payload.getData).map(writer.write)
        payload.release()
        Mono.empty()
      }
    })
}
