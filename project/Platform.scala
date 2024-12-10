package optimus

import optimus.Dependencies._
import sbt._
import sbt.Keys._
import sbtprotobuf.ProtobufPlugin
import sbtprotobuf.ProtobufPlugin.autoImport._
import sbtassembly.AssemblyPlugin.autoImport._

object Platform {
  private val projectsDir = file("optimus/platform/projects")
  private val loomSettings = Seq(
    Compile / unmanagedSourceDirectories += (Compile / sourceDirectory).value / "loom-off",
  )

  lazy val platform = Project("platform", projectsDir / "platform")
    .settings(
      scalacOptions ++= ScalacOptions.common ++ ScalacOptions.macros ++ ScalacOptions.dynamics ++ Seq("-P:entity:enableStaging:true"),
      libraryDependencies ++= Seq(freemarker, jacksonDatatypeJSR310, javaxMail)
    )
    .dependsOn(
      core,
      dalClient,
      entityPlugin,
      entityPluginJar % "plugin",
      priql,
      priqlDal,
      priqlDalPersistence
    )

  lazy val gitUtils = Project("platformGitUtils", projectsDir / "git-utils")
    .settings(libraryDependencies ++= Seq(scalaCollectionCompat, slf4j))

  lazy val bitBucketUtils = Project("platformBitBucketUtils", projectsDir / "bitbucket-utils")
    .settings(
      libraryDependencies ++= Seq(
        akkaHttpCore,
        akkaHttpSprayJson,
        Dependencies.sprayJson,
        typesafeConfig
      )
    )
    .dependsOn(restUtils)

  lazy val restUtils = Project("platformRestUtils", projectsDir / "rest-utils")
    .settings(libraryDependencies ++= Seq(Dependencies.sprayJson))

  lazy val priqlDalPersistence = Project("platformPriqlDalPersistence", projectsDir / "priql_dal_persistence")
    .settings(scalacOptions ++= ScalacOptions.common)
    .dependsOn(priqlDal)

  lazy val priqlDal = Project("platformPriqlDal", projectsDir / "priql_dal")
    .enablePlugins(ProtobufPlugin)
    .settings(
      ProtobufConfig / sourceDirectory := (Compile / resourceDirectory).value,
      scalacOptions ++= ScalacOptions.common ++ ScalacOptions.macros
    )
    .dependsOn(dalClient, dalCore, entityPlugin, entityPluginJar % "plugin", priql)

  lazy val priql = Project("platformPriql", projectsDir / "priql")
    .enablePlugins(ProtobufPlugin)
    .settings(
      ProtobufConfig / sourceDirectory := (Compile / resourceDirectory).value,
      scalacOptions ++= ScalacOptions.common ++ ScalacOptions.macros ++ Seq("-P:entity:enableStaging:true"),
      libraryDependencies ++= Seq(poi, scalaParserCombinator),
    )
    .dependsOn(
      alarms,
      core,
      coreMacro,
      collections,
      entityPlugin,
      entityPluginJar % "plugin",
      scalaCompat
    )

  lazy val dalClient = Project("platformDalClient", projectsDir / "dal_client")
    .settings(scalacOptions ++= ScalacOptions.common ++ ScalacOptions.macros)
    .dependsOn(core, dalCore, entityPlugin, entityPluginJar % "plugin", tls, versioningRuntime)

  lazy val versioningRuntime = Project("platformVersioningRuntime", projectsDir / "versioning_runtime")
    .settings(scalacOptions ++= ScalacOptions.common)
    .dependsOn(dalCore, entityPlugin, entityPluginJar % "plugin", core)

  /* lazy val msNetSSL = Project("platformMSNetSSL", projectsDir / "msnet-ssl")
    .dependsOn(missing) */

  lazy val tls = Project("platformTls", projectsDir / "tls")
    .settings(libraryDependencies ++= Seq(guava, nettyHandler, typesafeConfig))
    .dependsOn(missing, utils)

  lazy val dalCore = Project("platformDalCore", projectsDir / "dal_core")
    .settings(
      scalacOptions ++= ScalacOptions.common ++ ScalacOptions.macros,
      libraryDependencies ++= Seq(guice, logbackClassic, slf4j, springContext)
    )
    .dependsOn(
      breadcrumbs,
      collections,
      core,
      coreConfig,
      dalEnvironment,
      entityAgent,
      Silverking.silverking
    )

  lazy val collections = Project("platformCollections", projectsDir / "collections")

  lazy val dalEnvironment = Project("platformDalEnvironment", projectsDir / "dal_environment")
    .settings(
      scalacOptions ++= ScalacOptions.common,
      libraryDependencies ++= Seq(curatorFramework, guava, typesafeConfig)
    )
    .dependsOn(
      breadcrumbs,
      coreConfig,
      coreMacro,
      missing,
      Silverking.silverking
    )

  lazy val core = Project("platformCore", projectsDir / "core")
    .settings(
      scalacOptions ++= ScalacOptions.common ++ ScalacOptions.macros,
      libraryDependencies ++= Seq(
        asyncProfilerLoaderAll,
        commonsMath3,
        eaioUUID,
        fastUtil,
        jodaTime,
        Dependencies.sprayJson,
        typesafeConfig
      )
    )
    .dependsOn(
      annotations,
      breadcrumbs,
      coreConfig,
      debugger,
      entityAgent,
      GSF.breadcrumbs,
      inputs,
      instrumentation,
      missing,
      sprayJson,
      stagingPluginJar % "plugin"
    )

  lazy val debugger = Project("platformDebugger", projectsDir / "debugger")
    .settings(libraryDependencies ++= Seq(jacksonDatabind, jacksonModuleScala, junit % Test))
  
  lazy val instrumentation = Project("platformInstrumentation", projectsDir / "instrumentation")
    .settings(
      scalacOptions ++= ScalacOptions.common,
      libraryDependencies ++= Seq(
        asyncProfilerLoaderAll,
        httpClient,
        httpMime,
        kafka,
        springWeb
      )
    )
    .dependsOn(
      breadcrumbs,
      entityAgent,
      utils,
      stagingPluginJar % "plugin"
    )

  lazy val inputs = Project("platformInputs", projectsDir / "inputs")

  lazy val coreConfig = Project("platformCoreConfig", projectsDir / "core_config")
    .dependsOn(breadcrumbs, utils)

  lazy val entityAgent = Project("platformEntityAgent", projectsDir / "entityagent")
    .settings(
      loomSettings,
      libraryDependencies ++= Seq(asm, asmCommons, asmTree, asmUtil)
    )
    .dependsOn(entityAgentExt)

  lazy val entityAgentExt = Project("platformEntityAgentExt", projectsDir / "entityagent-ext")

  // fat jar version of entityPlugin, to be consumed as a compiler plugin
  lazy val entityPluginJar = Project("platformEntityPluginJar", projectsDir / "entityplugin-jar")
    .settings(
      exportJars := true,
      Compile / packageBin := (entityPlugin / assembly).value,
    )

  lazy val entityPlugin = Project("platformEntityPlugin", projectsDir / "entityplugin")
    .settings(
      loomSettings,
      scalacOptions ++= Seq("-language:postfixOps"),
      libraryDependencies ++= Seq(scalaCompiler, typesafeConfig),
      assembly / assemblyOption ~= { _.withIncludeScala(false) },
      assemblyMergeStrategy := {
        case "scalac-plugin.xml" => MergeStrategy.preferProject
        case other => assemblyMergeStrategy.value(other)
      } 
    )
    .dependsOn(
      scalaCompat,
      stagingPlugin,
      stagingPluginJar % "plugin"
    )

  lazy val breadcrumbs = Project("platformBreadcrumbs", projectsDir / "breadcrumbs")
    .settings(
      scalacOptions ++= ScalacOptions.common,
      libraryDependencies ++= Seq(
        archaiusCore,
        base64,
        httpClient,
        jacksonDatabind,
        jacksonModuleScala,
        kafkaClients,
        scalaCollectionCompat,
        snakeYaml,
        Dependencies.sprayJson,
      )
    )
    .dependsOn(missing, utils)

  lazy val missing = Project("platformMissing", projectsDir / "missing")
    .settings(libraryDependencies ++= Seq(curatorFramework, slf4j))

  lazy val utils = Project("platformUtils", projectsDir / "utils")
    .settings(
      scalacOptions ++= ScalacOptions.common ++ ScalacOptions.macros,
      libraryDependencies ++= Seq(
        args4j,
        caffeine,
        commonsIO,
        curatorRecipes,
        curatorFramework,
        guava,
        hkdf,
        javaxActivation,
        javaxMail,
        jettison,
        logbackClassic,
        openCSV,
        slf4j,
        zooKeeper,
        zstdJni
      )
    )
    .dependsOn(coreMacro, scalaCompat , stagingPlugin)

  lazy val scalaCompat = Project("platformScalaCompat", projectsDir / "scala_compat")
    .settings(
      libraryDependencies ++= Seq(
        scalaCollectionCompat,
        scalaCompiler,
        scalaParallelCollections,
        scalaXml
      )
    )

  // fat jar version of entityPlugin, to be consumed as a compiler plugin
  lazy val stagingPluginJar = Project("platformStagingPluginJar", projectsDir / "stagingplugin-jar")
    .settings(
      exportJars := true,
      Compile / packageBin := (stagingPlugin / assembly).value,
    )

  lazy val stagingPlugin = Project("platformStagingPlugin", projectsDir / "stagingplugin")
    .settings(
      libraryDependencies ++= Seq(scalaCompiler),
      assembly / assemblyOption ~= { _.withIncludeScala(false) }
    )
    .dependsOn(alarms, sprayJson)

  lazy val coreMacro = Project("platformCoreMacro", projectsDir / "core_macro")
    .settings(
      scalacOptions ++= Seq("-language:experimental.macros"),
      libraryDependencies ++= Seq(logbackClassic, logbackCore, scalaReflect, slf4j)
    )
    .dependsOn(alarms)

  lazy val alarms = Project("platformAlarms", projectsDir / "alarms")
    .settings(
      exportJars := true,
      libraryDependencies ++= Seq(scalaCompiler)
    )

  lazy val annotations = Project("platformAnnotations", projectsDir / "annotations")

  lazy val sprayJson = Project("platformSprayJson", projectsDir / "spray_json")
    .settings(
      exportJars := true,
      libraryDependencies += scalaReflect
    )
}
