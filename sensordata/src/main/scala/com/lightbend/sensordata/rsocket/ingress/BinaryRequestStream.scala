package com.lightbend.sensordata.rsocket.ingress

import cloudflow.akkastream._
import cloudflow.examples.sensordata.rsocket.avro.SensorData
import cloudflow.streamlets.avro.AvroOutlet
import cloudflow.streamlets.{ CodecOutlet, StreamletShape }
import com.lightbend.rsocket.dataconversion.SensorDataConverter
import io.rsocket._
import io.rsocket.core.RSocketServer
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.transport.netty.server._
import io.rsocket.util.{ ByteBufPayload, DefaultPayload }
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import reactor.core.publisher._

class BinaryRequestStream extends AkkaServerStreamlet {

  val out = AvroOutlet[SensorData]("out")

  def shape = StreamletShape.withOutlets(out)

  override def createLogic() = new RSocketStreamRequestLogic(this, out)
}

class RSocketStreamRequestLogic(server: Server, outlet: CodecOutlet[SensorData])
  (implicit context: AkkaStreamletContext) extends ServerStreamletLogic(server) {

  override def run(): Unit = {
    RSocketServer
      .create(new BinaryStreamAcceptor(sinkRef(outlet)))
      .payloadDecoder(PayloadDecoder.ZERO_COPY)
      .bind(WebsocketServerTransport.create("0.0.0.0", containerPort))
      .block()
    println(s"Bound RSocket server to port $containerPort")
  }
}

class BinaryStreamAcceptor(writer: WritableSinkRef[SensorData]) extends SocketAcceptor {

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] = {
    Mono.just(new RSocket {}).doFinally((_) ⇒ {
      reactiveSocket
        .requestStream(ByteBufPayload.create("Please may I have a stream"))
        .subscribe(new BinaryStreamBackpressureSubscriber(writer))
    })
  }
}

class BinaryStreamBackpressureSubscriber(writer: WritableSinkRef[SensorData]) extends BaseSubscriber[Payload] {
  private val log = LoggerFactory.getLogger(this.getClass)
  val NUMBER_OF_REQUESTS_TO_PROCESS: Long = 5
  var receivedItems = 0

  // Invoked on subscription, just specify the batch size
  override def hookOnSubscribe(subscription: Subscription): Unit = {
    subscription.request(NUMBER_OF_REQUESTS_TO_PROCESS)
  }

  // Invoked on next incoming request
  override def hookOnNext(value: Payload): Unit = {
    // Process request
    SensorDataConverter(value.getData).map(writer.write)
    value.release()
    // Ask for the next batch
    receivedItems += 1
    if (receivedItems % NUMBER_OF_REQUESTS_TO_PROCESS == 0)
      request(NUMBER_OF_REQUESTS_TO_PROCESS)
  }

  // Invoked on stream completion
  override def hookOnComplete(): Unit = log.info("Completing subscription")

  // Invoked on stream error
  override def hookOnError(throwable: Throwable): Unit = log.error(s"Stream subscription error [$throwable]")

  // Invoked on stream cancelation
  override def hookOnCancel(): Unit = log.info("Subscription canceled")
}
