package com.lightbend.sensordata

import cloudflow.akkastream._
import cloudflow.examples.sensordata.rsocket.avro.SensorData
import cloudflow.streamlets.{ CodecOutlet, StreamletShape }
import cloudflow.streamlets.avro.AvroOutlet
import com.lightbend.sensordata.support.DataConverter
import io.rsocket._
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server._
import io.rsocket.util.DefaultPayload
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
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

  // Data converter
  val dataConverter = new DataConverter[out](schema)

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] = {
    reactiveSocket
      .requestStream(DefaultPayload.create("Please may I have a stream"))
      .subscribe(new BaseSubscriber[Payload] {
        // Backpressure
        private val log = LoggerFactory.getLogger(this.getClass)
        // Number of request to get in a batch
        val NUMBER_OF_REQUESTS_TO_PROCESS = 5l
        var receivedItems = 0
        // Invoked on subscription, just specify the batch size
        override def hookOnSubscribe(subscription: Subscription): Unit = {
          subscription.request(NUMBER_OF_REQUESTS_TO_PROCESS)
        }
        // Invoked on next incoming request
        override def hookOnNext(value: Payload): Unit = {
          // Process request
          dataConverter.fromByteBuffer(value.getData).map(writer.write)
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
      })
    Mono.empty()
  }
}
