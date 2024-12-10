package optimus

import optimus.Dependencies._
import sbt._
import sbt.Keys._
import sbtprotobuf.ProtobufPlugin
import sbtprotobuf.ProtobufPlugin.autoImport._

object DHT {
  private val projectsDir = file("optimus/dht/projects")
  
  lazy val client3 = Project("dhtClient3", projectsDir / "client3")
    .settings(libraryDependencies ++= Seq(guiceAssistedInject, nettyResolverDNS))
    .dependsOn(digest, common3)

  val digest = Project("dhtDigest", projectsDir / "digest")

  lazy val common3 = Project("dhtCommon3", projectsDir / "common3")
    .enablePlugins(ProtobufPlugin)
    .settings(
      ProtobufConfig / sourceDirectory := (Compile / sourceDirectory).value / "proto",
      libraryDependencies ++= Seq(
        commonsLang3,
        guava,
        guice,
        hdrHistogram,
        jakartaInject,
        jcTools,
        nettyBuffer,
        nettyHandler,
        slf4j,
        snakeYaml,
        typesafeConfig,
        zooKeeper,
      )
    )
    .dependsOn(Platform.missing)
}