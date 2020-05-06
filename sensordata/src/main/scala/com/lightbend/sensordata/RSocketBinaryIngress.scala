package com.lightbend.sensordata

import akka.stream._
import cloudflow.akkastream.{ Server, _ }
import cloudflow.examples.sensordata.rsocket.avro._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import com.lightbend.sensordata.support.SensorDataConverter
import io.rsocket._
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server.TcpServerTransport
import reactor.core.publisher._

class RSocketBinaryIngress extends AkkaServerStreamlet {

  val out = AvroOutlet[SensorData]("out").withPartitioner(RoundRobinPartitioner)

  def shape = StreamletShape.withOutlets(out)

  override def createLogic() = new RSocketBinaryStreamletLogic(this, out)
}

class RSocketBinaryStreamletLogic(server: Server, outlet: CodecOutlet[SensorData])
  (implicit context: AkkaStreamletContext) extends ServerStreamletLogic(server) {

  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  override def run(): Unit = {
    RSocketServer.create(new RSocketBinaryAcceptorImpl(sinkRef(outlet)))
      .bind(TcpServerTransport.create("0.0.0.0", containerPort))
      .subscribe
    println(s"Bound RSocket server to port $containerPort")
  }
}

class RSocketBinaryAcceptorImpl(writer: WritableSinkRef[SensorData]) extends SocketAcceptor {

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        SensorDataConverter.fromByteBuffer(payload.getData).map(writer.write)
        Mono.empty()
      }
    })
}
