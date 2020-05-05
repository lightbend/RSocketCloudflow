package com.lightbend.sensordata

import akka.stream._
import akka.stream.stage._
import akka.stream.scaladsl._
import cloudflow.examples.sensordata.rsocket.avro._
import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.streamlets.avro._
import cloudflow.streamlets._
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import io.rsocket._
import reactor.core.publisher._
import com.lightbend.sensordata.support._
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server.TcpServerTransport

import scala.collection.mutable.ListBuffer

class RSocketIngress extends AkkaServerStreamlet {

  val out: AvroOutlet[SensorData] = AvroOutlet[SensorData]("out").withPartitioner(RoundRobinPartitioner)

  def shape: StreamletShape = StreamletShape.withOutlets(out)

  // Create Logic
  override final def createLogic = new RunnableGraphStreamletLogic() {
    // Runnable graph
    def runnableGraph = Source.fromGraph(new RSocketSource()).to(plainSink(out))
  }
}

class RSocketSource extends GraphStage[SourceShape[SensorData]] {
  // the outlet port of this stage which produces Ints
  val out: akka.stream.Outlet[SensorData] = Outlet("RsocketSource")

  override val shape: SourceShape[SensorData] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      override def preStart(): Unit = {
        RSocketServer.create(RSocketAcceptorImpl())
          .bind(TcpServerTransport.create("localhost", 3000))
          .subscribe
        ()
      }

      // create a handler for the outlet
      setHandler(out, new OutHandler {
        val acceptor = RSocketAcceptorImpl()
        // when you are asked for data (pulled for data)
        override def onPull(): Unit = {
          // emit an element on the outlet, if exists
          acceptor.nextSensorData() match {
            case Some(data) ⇒ push(out, data)
            case _          ⇒
          }
        }
      })
    }
}

object RSocketAcceptorImpl {
  private var acceptor: RSocketAcceptorImpl = null

  def apply(): RSocketAcceptorImpl = {
    if (acceptor == null)
      acceptor = new RSocketAcceptorImpl()
    acceptor
  }

}

class RSocketAcceptorImpl extends SocketAcceptor {

  private val sensorRecordInjection: Injection[SensorData, Array[Byte]] = SpecificAvroCodecs.toBinary(SensorData.SCHEMA$)
  private val sensorAvroSerde: AvroSerde[SensorData] = new AvroSerde(sensorRecordInjection)

  private val data = new ListBuffer[SensorData]()

  def nextSensorData(): Option[SensorData] = this.synchronized {
    data.isEmpty match {
      case false ⇒
        val element = data.head
        data.drop(1)
        Some(element)
      case true ⇒ Option.empty
    }
  }

  def addSensorData(sensor: Array[Byte]): Unit = {
    try {
      val avroSensor = sensorAvroSerde.decode(sensor)
      this.synchronized {
        data += avroSensor
      }
      ()
    } catch {
      case t: Throwable ⇒
        t.printStackTrace()
    }
  }

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        addSensorData(payload.data().array())
        Mono.empty()
      }
    })
}
