import Versions._
import sbt._

object Dependencies {

  val rsocketCore           = "io.rsocket"          % "rsocket-core"                      % RSocketVersion
  val rsocketTransport      = "io.rsocket"          % "rsocket-transport-netty"           % RSocketVersion
  val rsocketLocal          = "io.rsocket"          % "rsocket-transport-local"           % RSocketVersion
  val rsocketBalancer       = "io.rsocket"          % "rsocket-load-balancer"             % RSocketVersion

  val argona                = "org.agrona"          % "Agrona"                            % argonaVersion

  val reactorKafka          = "io.projectreactor.kafka" % "reactor-kafka"                 % reactorKafkaVersion
  val kafka                 = "org.apache.kafka"    %% "kafka"                            % kafkaVersion
  val curator               = "org.apache.curator"  % "curator-test"                      % curatorVersion
  val commonIO              = "commons-io"          % "commons-io"                        % commonIOVersion

  val chronicle             = "net.openhft"         % "chronicle-queue"                   % chronicleVersion

  val slf4                  = "org.slf4j"           % "slf4j-api"                         % SLFVersion
  val logback               = "ch.qos.logback"      % "logback-classic"                   % LogBackVersion

  val marshallers           = "com.typesafe.akka"   %% "akka-http-spray-json"             % marshallersVersion

  val akkastream            = "com.typesafe.akka"   %% "akka-stream"                      % akkaVersion

  val typesafeConfig        = "com.typesafe"        %  "config"                           % TypesafeConfigVersion
  val ficus                 = "com.iheart"          %% "ficus"                            % FicusVersion

  val scalaTest             = "org.scalatest"       %% "scalatest"                        % scaltestVersion    % "test"

}
