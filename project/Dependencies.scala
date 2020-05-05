import Versions._
import sbt._

object Dependencies {

  val rsocketCore           = "io.rsocket"              % "rsocket-core"                        % RSocketVersion
  val rsocketTransport      = "io.rsocket"              % "rsocket-transport-netty"             % RSocketVersion
  val slf4                  = "org.slf4j"               % "slf4j-api"                           % SLFVersion
  val logback               = "ch.qos.logback"          % "logback-classic"                     % LogBackVersion

  val typesafeConfig        = "com.typesafe"            %  "config"                             % TypesafeConfigVersion
  val ficus                 = "com.iheart"              %% "ficus"                              % FicusVersion

  val scalaTest             = "org.scalatest"           %% "scalatest"                          % scaltestVersion    % "test"

}
