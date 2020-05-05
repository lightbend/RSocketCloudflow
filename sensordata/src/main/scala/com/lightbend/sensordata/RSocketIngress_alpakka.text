package com.lightbend.sensordata

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import cloudflow.akkastream._
import cloudflow.akkastream.scaladsl._
import cloudflow.examples.sensordata.rsocket.avro._
import cloudflow.streamlets._
import cloudflow.streamlets.avro._
import io.rsocket._
import io.rsocket.core.RSocketServer
import io.rsocket.transport.netty.server.TcpServerTransport
import reactor.core.publisher._

import scala.collection.mutable.ListBuffer

class RSocketIngress extends AkkaServerStreamlet with SprayJsonSupport {

  val out: AvroOutlet[SensorData] = AvroOutlet[SensorData]("out").withPartitioner(RoundRobinPartitioner)

  def shape: StreamletShape = StreamletShape.withOutlets(out)

  // Create Logic
  override final def createLogic = new RunnableGraphStreamletLogic() {
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
    // Runnable graph
    def runnableGraph = Source.fromGraph(new RSocketSource()).map(FromByteStringUnmarshaller).to(plainSink(out))
  }
}

class RSocketSource extends GraphStage[SourceShape[String]] {

  // the outlet port of this stage which produces Ints
  val out: akka.stream.Outlet[String] = Outlet("RsocketSource")

  override val shape: SourceShape[String] = SourceShape(out)

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
            case Some(data) ⇒
              push(out, data)
              println("Producing a new data element")
            case _ ⇒
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

  private val data = new ListBuffer[String]()

  def nextSensorData(): Option[String] = this.synchronized{
    data.isEmpty match {
      case false ⇒
        val element = data.head
        data.drop(1)
        Some(element)
      case true ⇒ Option.empty
    }
  }

  def addSensorData(sensor: String): Unit = {
    println("Adding a new FF message")
    this.synchronized {
      data += sensor
    }
    println("New element in the queue")
  }

  override def accept(setupPayload: ConnectionSetupPayload, reactiveSocket: RSocket): Mono[RSocket] =
    Mono.just(new AbstractRSocket() {
      override def fireAndForget(payload: Payload): Mono[Void] = {
        println("Got a new FF message")
        try {
          val data = payload.getDataUtf8
          println(s"Retrieved $data from a payload")
          addSensorData(data)
        } catch {
          case t: Throwable ⇒
            t.printStackTrace()
        }
        Mono.empty()
      }
    })
}
