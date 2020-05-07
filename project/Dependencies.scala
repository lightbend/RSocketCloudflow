import Versions._
import sbt._

object Dependencies {

  val rsocketCore           = "io.rsocket"              % "rsocket-core"                        % RSocketVersion
  val rsocketTransport      = "io.rsocket"              % "rsocket-transport-netty"             % RSocketVersion
  val rsocketBalancer       = "io.rsocket"              % "rsocket-load-balancer"               % RSocketVersion
  val slf4                  = "org.slf4j"               % "slf4j-api"                           % SLFVersion
  val logback               = "ch.qos.logback"          % "logback-classic"                     % LogBackVersion

  val marshallers           = "com.typesafe.akka"       %% "akka-http-spray-json"               % marshallersVersion

  val scalaTest             = "org.scalatest"           %% "scalatest"                          % scaltestVersion    % "test"

}
