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
import SensorDataJsonSupport._
import io.rsocket.frame.decoder.PayloadDecoder

class UTF8FireAndForget extends AkkaServerStreamlet with SprayJsonSupport {

  val out = AvroOutlet[SensorData]("out")

  def shape = StreamletShape.withOutlets(out)

  // Create logic
  override def createLogic() = new UTF8FireAndForgetStreamletLogic(this, out)
}

// Logic definition
class UTF8FireAndForgetStreamletLogic(server: Server, outlet: CodecOutlet[SensorData])
  (implicit context: AkkaStreamletContext, fbu: FromByteStringUnmarshaller[SensorData]) extends ServerStreamletLogic(server) {

  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  override def run(): Unit = {
    // Start server
    RSocketServer.create(new UTF8FireAndForgetAcceptor(sinkRef(outlet)))
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(TcpServerTransport.create("0.0.0.0", containerPort))
      .subscribe
    println(s"Bound RSocket server to port $containerPort")
  }
}

class UTF8FireAndForgetAcceptor(writer: WritableSinkRef[SensorData])
  (implicit
    fbu: FromByteStringUnmarshaller[SensorData],
   ec: ExecutionContext, mat: ActorMaterializer) extends SocketAcceptor {

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new RSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        // Get data
        val data = ByteString(payload.getDataUtf8)
        // Convert and publish
        fbu.apply(data).flatMap(writer.write)
        Mono.empty()
      }
    })
}
