
import optimus.BuildTool
import optimus.Dependencies
import optimus.DHT
import optimus.GSF
import optimus.Platform
import optimus.Silverking
import optimus.Stratosphere

ThisBuild / scalaVersion := Dependencies.scala2Version

val buildToolApp = BuildTool.app
val buildToolRest = BuildTool.rest
val buildToolRunConf = BuildTool.runConf
val builtToolFormat = BuildTool.format
val buildToolCore = BuildTool.core

val dhtClient3 = DHT.client3
val dhtDigest = DHT.digest
val dhtCommon3 = DHT.common3

val gsfBreadcrumbs = GSF.breadcrumbs

val platform = Platform.platform
val platformGitUtils = Platform.gitUtils
val platformBitBucketUtils = Platform.bitBucketUtils
val platformRestUtils = Platform.restUtils
val platformPriqlDalPersistence = Platform.priqlDalPersistence
val platformPriqlDal = Platform.priqlDal
val platformPriql = Platform.priql
val platformDalClient = Platform.dalClient
// val platformMSNetSSL = Platform.msNetSSL
val platformVersioningRuntime = Platform.versioningRuntime
val platformTls = Platform.tls
val platformDalCore = Platform.dalCore
val platformDalEnvironment = Platform.dalEnvironment
val platformCollections = Platform.collections
val platformCore = Platform.core
val platformDebugger = Platform.debugger
val platformInstrumentation = Platform.instrumentation
val platformInputs = Platform.inputs
val platformCoreConfig = Platform.coreConfig
val platformEntityAgent = Platform.entityAgent
val platformEntityAgentExt = Platform.entityAgentExt
val platformEntityPlugin = Platform.entityPlugin
val platformEntityPluginJar = Platform.entityPluginJar
val platformBreadcrumbs = Platform.breadcrumbs
val platformMissing = Platform.missing
val platformUtils = Platform.utils
val platformScalaCompat = Platform.scalaCompat
val platformStagingPlugin = Platform.stagingPlugin
val platformStagingPluingJar = Platform.stagingPluginJar
val platformCoreMacro = Platform.coreMacro
val platformAlarms = Platform.alarms
val platformAnnotations = Platform.annotations
val platformSprayJson = Platform.sprayJson

val silverking = Silverking.silverking
val silverkingUtils = Silverking.utils

val stratosphereBootstrap = Stratosphere.bootstrap
val stratosphereCommon = Stratosphere.common
