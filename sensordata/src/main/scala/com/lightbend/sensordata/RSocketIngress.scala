package com.lightbend.sensordata

import akka.stream._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.unmarshalling.FromByteStringUnmarshaller
import akka.util.ByteString
import cloudflow.examples.sensordata.rsocket.avro._
import cloudflow.akkastream.{ Server, _ }
import cloudflow.streamlets.avro._
import cloudflow.streamlets._
import io.rsocket._
import reactor.core.publisher._
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server.TcpServerTransport

import scala.concurrent.ExecutionContext
import SensorDataJsonSupport._

class RSocketIngress extends AkkaServerStreamlet with SprayJsonSupport {

  val out = AvroOutlet[SensorData]("out")

  def shape = StreamletShape.withOutlets(out)

  // Create logic
  override def createLogic() = new RSocketStreamletLogic[SensorData](this, out)
}

// Logic definition
class RSocketStreamletLogic[Out](server: Server, outlet: CodecOutlet[Out])
  (implicit context: AkkaStreamletContext, fbu: FromByteStringUnmarshaller[Out]) extends ServerStreamletLogic(server) {

  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  override def run(): Unit = {
    // Start server
    RSocketServer.create(new RSocketAcceptorImpl[Out](sinkRef(outlet)))
      .bind(TcpServerTransport.create("0.0.0.0", containerPort))
      .subscribe
    println(s"Bound RSocket server to port $containerPort")
  }
}

class RSocketAcceptorImpl[Out](writer: WritableSinkRef[Out])
  (implicit
    fbu: FromByteStringUnmarshaller[Out],
   ec: ExecutionContext, mat: ActorMaterializer) extends SocketAcceptor {

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        // Get data
        val data = ByteString(payload.getDataUtf8)
        // Convert and publish
        fbu.apply(data).flatMap(writer.write)
        Mono.empty()
      }
    })
}
