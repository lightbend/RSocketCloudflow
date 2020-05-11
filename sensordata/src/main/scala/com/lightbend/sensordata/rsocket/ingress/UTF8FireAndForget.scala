package com.lightbend.sensordata.rsocket.ingress

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.unmarshalling.FromByteStringUnmarshaller
import akka.stream._
import akka.util.ByteString
import cloudflow.akkastream.{ Server, _ }
import cloudflow.examples.sensordata.rsocket.avro._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import io.rsocket._
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server.TcpServerTransport
import reactor.core.publisher._

import scala.concurrent.ExecutionContext
import com.lightbend.sensordata.SensorDataJsonSupport._

class UTF8FireAndForget extends AkkaServerStreamlet with SprayJsonSupport {

  val out = AvroOutlet[SensorData]("out")

  def shape = StreamletShape.withOutlets(out)

  // Create logic
  override def createLogic() = new UTF8FireAndForgetStreamletLogic[SensorData](this, out)
}

// Logic definition
class UTF8FireAndForgetStreamletLogic[Out](server: Server, outlet: CodecOutlet[Out])
  (implicit context: AkkaStreamletContext, fbu: FromByteStringUnmarshaller[Out]) extends ServerStreamletLogic(server) {

  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  override def run(): Unit = {
    // Start server
    RSocketServer.create(new UTF8FireAndForgetAcceptor[Out](sinkRef(outlet)))
      .bind(TcpServerTransport.create("0.0.0.0", containerPort))
      .subscribe
    println(s"Bound RSocket server to port $containerPort")
  }
}

class UTF8FireAndForgetAcceptor[Out](writer: WritableSinkRef[Out])
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
