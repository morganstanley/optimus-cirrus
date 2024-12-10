package optimus

import optimus.Dependencies._
import sbt._
import sbt.Keys._
import sbtprotobuf.ProtobufPlugin
import sbtprotobuf.ProtobufPlugin.autoImport._

object Stratosphere {
  private val projectsDir = file("optimus/stratosphere")

  lazy val common = Project("stratosphereCommon", projectsDir / "common")
    .settings(
      scalacOptions ++= ScalacOptions.common,
      libraryDependencies ++= Seq(
        akkaActor,
        akkaHttpCore,
        akkaStream,
        args4j,
        commonsCompress,
        hamcrest,
        jacksonDatabind,
        jansi,
        javaxActivation,
        jnaPlatform,
        junit,
        scalaReflect,
        scalaCollectionCompat,
        sprayJson
      )
    )
    .dependsOn(
      bootstrap,
      Platform.bitBucketUtils,
      Platform.stagingPluginJar % "plugin",
      Platform.utils
    )

  lazy val bootstrap = Project("stratosphereBootstrap", projectsDir / "bootstrap")
    .settings(libraryDependencies ++= Seq(typesafeConfig))
}