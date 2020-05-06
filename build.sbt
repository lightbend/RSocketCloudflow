import sbt._
import sbt.Keys._
import Dependencies._
import scalariform.formatter.preferences._

name := "RSocket"

lazy val thisVersion = "0.1"
organization in ThisBuild := "lightbend"
version in ThisBuild := thisVersion
scalaVersion in ThisBuild := "2.12.10"

// settings for a native-packager based docker project based on sbt-docker plugin
def sbtdockerAppBase(id: String)(base: String = id): Project = Project(id, base = file(base))
  .enablePlugins(sbtdocker.DockerPlugin, JavaAppPackaging)
  .settings(
    dockerfile in docker := {
      val appDir = stage.value
      val targetDir = "/opt/app"

      new Dockerfile {
        from("lightbend/java-bash-base:0.0.1")
        copy(appDir, targetDir)
        run("chmod", "-R", "777", "/opt/app")
        entryPoint(s"$targetDir/bin/${executableScriptName.value}")
      }
    },

    // Set name for the image
    imageNames in docker := Seq(
      ImageName(namespace = Some(organization.value),
        repository = name.value.toLowerCase,
        tag = Some(version.value))
    ),

    buildOptions in docker := BuildOptions(cache = false)
  )

lazy val rsocketproducer = sbtdockerAppBase("rsocket-data-publisher")("./rsocketproducer")
  .settings(
    mainClass in Compile := Some("com.lightbend.sensordata.producer.RSocketPublisherBinary"),
    libraryDependencies ++= Seq(rsocketCore, rsocketTransport, slf4, logback)
  )
  .dependsOn(support)

lazy val interactions = (project in file("./interactions"))
  .settings(libraryDependencies ++= Seq(rsocketCore, rsocketTransport, slf4, logback))

lazy val support = (project in file("./support"))
  .enablePlugins(CloudflowAkkaStreamsLibraryPlugin)
  .settings(
    name := "support",
    version := thisVersion,
    libraryDependencies ++= Seq(logback, scalaTest),
  )
  .settings(commonSettings)

lazy val sensorData = (project in file("./sensordata"))
  .enablePlugins(CloudflowApplicationPlugin)
  .settings(
    name := "sensordata",
    version := thisVersion,
    libraryDependencies ++= Seq(marshallers, rsocketCore, rsocketTransport, slf4, logback)
  )
  .settings(commonSettings)
  .dependsOn(support)


lazy val commonScalacOptions = Seq(
  "-encoding", "UTF-8",
  "-target:jvm-1.8",
  "-Xlog-reflective-calls",
  "-Xlint:_",
  "-deprecation",
  "-feature",
  "-language:_",
  "-unchecked"
)

lazy val scalacTestCompileOptions = commonScalacOptions ++ Seq(
  //  "-Xfatal-warnings",                  // Avro generates unused imports, so this is commented out not to break build
  "-Ywarn-dead-code",                  // Warn when dead code is identified.
  "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
  "-Ywarn-numeric-widen",              // Warn when numerics are widened.
  "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
  "-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
  "-Ywarn-unused:locals",              // Warn if a local definition is unused.
  "-Ywarn-unused:params",              // Warn if a value parameter is unused. (But there's no way to suppress warning when legitimate!!)
  "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates",            // Warn if a private member is unused.
)

lazy val scalacSrcCompileOptions = scalacTestCompileOptions ++ Seq(
  "-Ywarn-value-discard")

lazy val commonSettings = Seq(
  scalacOptions in Compile := scalacSrcCompileOptions,
  scalacOptions in Test := scalacTestCompileOptions,
  scalacOptions in (Compile, console) := commonScalacOptions,
  scalacOptions in (Test, console) := commonScalacOptions,

  scalariformPreferences := scalariformPreferences.value
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 90)
    .setPreference(DoubleIndentConstructorArguments, true)
    .setPreference(DoubleIndentMethodDeclaration, true)
    .setPreference(IndentLocalDefs, true)
    .setPreference(IndentPackageBlocks, true)
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(DanglingCloseParenthesis, Preserve)
    .setPreference(NewlineAtEndOfFile, true)
    .setPreference(AllowParamGroupsOnNewlines, true)
    .setPreference(SpacesWithinPatternBinders, false) // otherwise case head +: tail@_ fails to compile!
)