package optimus

import optimus.Dependencies._
import sbt._
import sbt.Keys._
import sbtprotobuf.ProtobufPlugin
import sbtprotobuf.ProtobufPlugin.autoImport._

object Silverking {
  private val projectsDir = file("optimus/silverking/projects")
  
  lazy val silverking = Project("silverking", projectsDir / "silverking")
    .enablePlugins(ProtobufPlugin)
    .settings(
      ProtobufConfig / sourceDirectory := (Compile / resourceDirectory).value,
      libraryDependencies ++= Seq(
        awsSdk,
        caliper,
        commonsCompress,
        fastUtil,
        hibernateValidator,
        jacksonDataformatYaml,
        jgrapht
      )
    )
    .dependsOn(Platform.breadcrumbs, utils)

  lazy val utils = Project("silverkingUtils", projectsDir / "silverking_utils")
    .settings(libraryDependencies ++= Seq(guava, slf4j))
}
