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

  override def run(): Unit = {
    val writer = sinkRef(outlet)
    // Create server
    RSocketServer.create(SocketAcceptor.forFireAndForget((payload: Payload) ⇒ {
      // Get data
      val data = ByteString(payload.getDataUtf8)
      // Convert and publish
      fbu.apply(data).flatMap(writer.write)
      payload.release()
      Mono.empty()
    }))
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(TcpServerTransport.create("0.0.0.0", containerPort))
      .subscribe
    println(s"Bound RSocket server to port $containerPort")
  }
}
