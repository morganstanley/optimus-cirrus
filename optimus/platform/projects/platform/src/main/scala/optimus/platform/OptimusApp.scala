/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.platform

import ch.qos.logback.classic.Logger
import msjava.tools.util.MSProcess
import optimus.breadcrumbs.BreadcrumbsSendLimit.OnceByCrumbEquality
import optimus.breadcrumbs._
import optimus.breadcrumbs.crumbs.Crumb.RuntimeSource
import optimus.breadcrumbs.crumbs._
import optimus.breadcrumbs.crumbs.{Properties => BCProps}
import optimus.config.OptconfProvider
import optimus.config.scoped.ScopedConfiguration
import optimus.config.scoped.ScopedSchedulerPlugin
import optimus.exceptions.ExceptionProxy
import optimus.graph.AsyncProfilerIntegration
import optimus.graph.AuxiliarySchedulerHelper
import optimus.graph.CollectionTraceBreadcrumbs
import optimus.graph.DiagnosticSettings
import optimus.graph.FileInterceptor
import optimus.graph.GCMonitor
import optimus.graph.GCNative
import optimus.graph.OGTrace
import optimus.graph.Settings
import optimus.graph.NodeTaskInfo
import optimus.graph.cache.Caches
import optimus.graph.cache.MemoryLeakCheckCacheClear
import optimus.graph.diagnostics.InfoDumper
import optimus.graph.diagnostics.gridprofiler.AggregationType
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.graph.diagnostics.gridprofiler.GridProfilerDefaults
import optimus.graph.diagnostics.gridprofiler.HotspotFilter
import optimus.graph.diagnostics.gridprofiler.Level.Level
import optimus.graph.diagnostics.heap.HeapSampling
import optimus.graph.diagnostics.messages.StartupEventCounter
import optimus.graph.diagnostics.sampling.SamplingProfiler
import optimus.graph.diagnostics.sampling.SamplingProfilerSource
import optimus.graph.diagnostics.trace.OGTraceMode
import optimus.graph.diagnostics.tsprofiler.TemporalSurfProfilingLevel.TemporalSurfProfilingLevel
import optimus.graph.diagnostics.tsprofiler.TemporalSurfaceProfilingUtils
import optimus.logging.HardwareInfo
import optimus.logging.LoggingInfo
import optimus.logging.OptimusLogging
import optimus.platform.OptimusApp.ExitHandler
import optimus.platform.appgroupinfo.AppGroupFinder
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalAsyncBatchingConfig
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalLocation
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dal.session.RolesetMode
import optimus.platform.dtc.CachedExecutionPlugin
import optimus.platform.dtc.CachedExecutionPlugin.CachingDisabled
import optimus.platform.dtc.CachedExecutionPlugin.ExecutionCached
import optimus.platform.dtc.CachedExecutionPlugin.NoCacheFound
import optimus.platform.inputs.GraphInputConfiguration
import optimus.platform.inputs.NodeInput
import optimus.platform.inputs.NodeInputResolver
import optimus.platform.inputs.ProcessState
import optimus.platform.inputs.loaders.LoaderSource
import optimus.platform.inputs.loaders.OptimusLoaders
import optimus.platform.inputs.loaders.OptimusNodeInputStorage
import optimus.platform.inputs.loaders.OptimusNodeInputStorage.OptimusStorageUnderlying
import optimus.platform.inputs.registry.CmdLineSource
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.platform.inputs.registry.Registry
import optimus.platform.inputs.registry.SharedProcessGraphInputNames
import optimus.platform.inputs.registry.SharedProcessGraphInputNames.hotspotFilterExplanation
import optimus.platform.inputs.registry.SharedProcessGraphInputNames.profileHotspotfilter
import optimus.platform.installcommonpath.InstallCommonPath
import optimus.platform.runtime.OptimusCompositeLeaderElectorClient
import optimus.platform.runtime.OptimusEarlyInitializer
import optimus.platform.runtime.ZkUtils
import optimus.platform.util.ArgHandlers.DelimitedStringOptionHandler
import optimus.platform.util.ArgumentPublisher
import optimus.platform.util.ArgumentPublisher.parseOptimusArgsToMap
import optimus.platform.util.AutoSysUtils
import optimus.platform.util.LoggingHelper
import optimus.platform.util.ProcessEnvironment
import optimus.platform.util.Version
import optimus.systemexit.ExitInterceptProp
import optimus.systemexit.SystemExitReplacement
import optimus.utils.SystemFinalization
import optimus.utils.TreadmillTools.treadmillInstance
import org.apache.commons.lang3.StringUtils
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.OptionDef
import org.kohsuke.args4j.spi.Messages
import org.kohsuke.args4j.spi.OneArgumentOptionHandler
import org.kohsuke.args4j.spi.Setter
import org.kohsuke.args4j.spi.StringArrayOptionHandler

import java.util.concurrent.CountDownLatch
import java.util.{List => JList}
import scala.annotation.nowarn
import scala.collection.mutable.ListBuffer
import scala.compat.Platform.currentTime
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Properties
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

class OptimusAppCmdLine {
  import org.kohsuke.args4j

  @args4j.Option(
    name = "-e",
    aliases = Array("--env"),
    usage = "runtime environment (mode:suffix)",
    handler = classOf[DalEnvironmentHandler])
  val env: String = null

  @args4j.Option(name = "-u", aliases = Array("--uri"), usage = "override default DSI URI")
  var uri: String = _

  @args4j.Option(name = "-help", aliases = Array("--help"), usage = "Print usage information")
  val help = false

  @args4j.Option(
    name = "-r",
    aliases = Array("--roleset"),
    usage =
      "comma-separated set of roles to try to assume for entitlements checks. If this argument is repeated, the first successful role set wins. The allRoles option has precedence over the roleset one, i.e. if both --allRoles and -r are specified, then -r has no effect."
  )
  val rolesets: JList[String] = new java.util.ArrayList[String]()

  @args4j.Option(
    name = "--allRoles",
    usage =
      "causes the roleset to be set to all available roles that the user is allowed to assume, i.e. all roles the user is a member of. The allRoles option has precedence over the roleset one, i.e. if both --allRoles and -r are specified, then -r has no effect."
  )
  val allRoles: Boolean = false

  @args4j.Option(
    name = "--dalSessionToken",
    usage = "a pipe or file containing a DAL session token to be used by distributed DAL query nodes")
  val sessionTokenLocation: String = null

  @args4j.Option(name = "-z", aliases = Array("--zone"), usage = "application's zone agreed upon with DAL")
  var zone: String =
    System.getProperty("ZONE_NAME", OptimusApp.systemEnv.getOrElse("ZONE_NAME", DalZoneId.default.underlying))

  @args4j.Option(
    name = "-app",
    aliases = Array("--app"),
    usage = "application's unique identifier which is defaulted from APP_NAME system property or environment variable")
  var appId: String = System.getProperty("APP_NAME", OptimusApp.systemEnv.getOrElse("APP_NAME", null))

  @args4j.Option(
    name = SharedProcessGraphInputNames.profileTSLevel,
    usage = SharedProcessGraphInputNames.profileTSLevelExplanation)
  val profileTSLevel: String = null

  @args4j.Option(
    name = SharedProcessGraphInputNames.profileTSFolder,
    usage = SharedProcessGraphInputNames.profileTSFolderExplanation)
  val profileTSFolder: String = null

  @args4j.Option(
    name = SharedProcessGraphInputNames.profileLevel,
    aliases = Array("--profileLevel"),
    usage = SharedProcessGraphInputNames.profileLevelExplanation)
  val profileLevel: String = null

  @args4j.Option(name = "--profile-csvfolder", usage = "folder for CSV output when --profile-graph is on")
  val profileCSVFolder: String = null

  @args4j.Option(
    name = "--profile-aggregation",
    usage = "default aggregation for profiler csv outputs",
    handler = classOf[AggregationType.OptionHandler])
  val profileAggregationType: AggregationType.Value = AggregationType.DEFAULT

  @args4j.Option(
    name = profileHotspotfilter,
    usage = hotspotFilterExplanation,
    handler = classOf[StringArrayOptionHandler])
  private[platform] val profileCustomHotspots = Array.empty[String]

  @args4j.Option(
    name = "--profile-custom-metrics",
    usage =
      "choose the custom metrics to be recorded, 'all' to enable all (default none). This list is comma separated",
    handler = classOf[DelimitedStringOptionHandler]
  )
  val profileCustomMetrics = Seq.empty[String]

  @args4j.Option(
    name = SharedProcessGraphInputNames.configGraph,
    usage = SharedProcessGraphInputNames.configGraphExplanation,
    handler = classOf[DelimitedStringOptionHandler]
  )
  val configGraph: Seq[String] = null

  @args4j.Option(
    name = SharedProcessGraphInputNames.startupConfig,
    usage = SharedProcessGraphInputNames.startupConfigExplanation)
  val startupConfig: String = null

  @args4j.Option(
    name = "--config-folder",
    usage = "output directory for .optconf file generated if hotspots profiling is on")
  val configFolder: String = profileCSVFolder

  @args4j.Option(
    name = "--pgo-mode",
    depends = Array("--pgo-folder"), // not required, but if specified then --pgo-folder becomes required
    usage = "Comma separated list specifying how to generate optconf (supported: disableCache, tweakUsage)",
    handler = classOf[DelimitedStringOptionHandler]
  )
  val pgoModes: Seq[String] = Seq.empty[String]

  @args4j.Option(
    name = "--pgo-folder",
    usage = "output directory for .optconf file generated if --pgo-mode is specified")
  val pgoFolder: String = profileCSVFolder

  @args4j.Option(name = "--async-profiler", usage = "Start async-profiler with app")
  val asyncProfiler: Boolean = false

  // These options are no-ops.  Here only because removing options in the startup scripts is difficult.
  @args4j.Option(name = "--jemalloc", hidden = true)
  val _jemallocOption = false
  @args4j.Option(name = "--tcmalloc", hidden = true)
  val _tcmallocOption = false
  @args4j.Option(name = "--no-mts", hidden = true)
  val _noMtsOption = false
  @args4j.Option(name = "--glibc", hidden = true)
  val _glibcOption = false

  @args4j.Option(
    name = "--exitMode",
    usage =
      "how to behave on app exit [NoAction | ForceExit | Suspend]. Use Suspend to prevent the process from going down when debugging",
    handler = classOf[ExitMode.OptionOptionHandler]
  )
  val exitMode: Option[ExitMode.Value] = None

  @args4j.Option(
    name = "--scopedConfiguration",
    usage = "path to the scoped configuration file to be used",
    handler = classOf[ScopedConfiguration.OptionOptionHandler]
  )
  val scopedConfiguration: Option[ScopedConfiguration] = None
}

object DalEnvironmentHandler {
  private val dalEnvRegex = "^[\\w-]+(\\:[\\w-]+)?$".r
}

private[optimus /*platform*/ ] class DalEnvironmentHandler(
    parser: CmdLineParser,
    option: OptionDef,
    setter: Setter[String])
    extends OneArgumentOptionHandler[String](parser, option, setter) {
  import DalEnvironmentHandler._
  import optimus.graph.DiagnosticSettings

  private val allowAnyDalEnvSyntax = DiagnosticSettings.getBoolProperty("optimus.app.allowAnyDalEnvSyntax", false)

  protected override def parse(argument: String): String = {
    if (allowAnyDalEnvSyntax || dalEnvRegex.pattern.matcher(argument).matches)
      argument
    else
      throw new CmdLineException(owner, Messages.ILLEGAL_OPERAND, option.toString, argument)
  }
}

trait OptimusAppTrait[Args <: OptimusAppCmdLine] extends OptimusTask {
  import OptimusAppTrait.log

  private[optimus /*platform*/ ] def manifest: Manifest[Args]
  var cmdLine: Args = _

  var parser: CmdLineParser = _

  protected def args: Array[String] = _args

  private var _args: Array[String] = _

  @volatile private[this] var returnCode: Int = 0

  protected val useInMemoryCaching: Boolean = false

  protected def residentZkaContexts: Boolean = false

  protected def appDryRun: Boolean = sys.props.get("optimus.app.dry.run").contains("true")
  protected def appLogJson: Boolean = sys.props.get("optimus.app.log.launch").contains("true")

  /**
   * Set the return code to be used on an orderly exit
   */
  protected final def setReturnCode(returnCode: Int): Unit = {
    this.returnCode = returnCode
  }

  /**
   * The type of desired behavior on return from the main method
   */
  protected def appExitMode: ExitMode.Value = ExitMode.NoAction
  protected final def exitMode: ExitMode.Value = {
    Option(cmdLine)
      .flatMap(_.exitMode)
      .orElse(Option(System.getProperty("optimus.app.exitMode")).map(ExitMode.withName))
      .orElse { if (DiagnosticSettings.diag_showConsole) Some(ExitMode.Suspend) else None }
      .getOrElse(appExitMode)
  }

  private val gc = GCMonitor.instance

  def exit(maybeException: Option[Exception], retCode: Int): Unit = {
    println(
      s"${maybeException.map(_ => "printing exception to stderr and ").getOrElse("")}exiting with return code $retCode")
    maybeException.foreach { exception =>
      exception.printStackTrace()
    }
    System.exit(retCode)
  }

  protected[optimus] def suspend(): Unit = new CountDownLatch(1).await()

  /** Hook for sundry cleanup actions such as flushing breadcrumbs/auxscheduler/profiling data. */
  protected def flushBeforeExit(): Unit = try {
    if (Breadcrumbs.collecting) {
      Breadcrumbs.send(OnceByCrumbEquality, ChainedID.root, new EventCrumb(_, RuntimeSource, Events.AppCompleted))

      val propertyMap = appLaunchContextInfo() + (BCProps.event -> Events.AppCompleted.name)
      Breadcrumbs.send(OnceByCrumbEquality, ChainedID.root, PropertiesCrumb(_, RuntimeSource, propertyMap))

      CollectionTraceBreadcrumbs.publishInfoIfNeeded(ChainedID.root, Events.OptimusAppCollected)
    }
    GridProfiler.logSummary(Option(cmdLine).flatMap(cl => Option(cl.appId)))
    TemporalSurfaceProfilingUtils.dumpTemporalSurfProfilingData()
    Breadcrumbs.flush()
    HeapSampling.exiting()
    AuxiliarySchedulerHelper.flush()
    if (GCNative.jemallocProfiling()) {
      if (GCNative.isLoaded) {
        for (level <- 0 until GCNative.LEVEL_SHUTDOWN) {
          GCNative.clearCache(level)
          InfoDumper.upload(Crumb.GCSource, GCNative.jemallocDump(s"shutdown-$level"))
        }
      } else {
        Caches.clearAllCaches(
          MemoryLeakCheckCacheClear,
          includeSI = true,
          includePropertyLocal = true,
          includeNamed = true)
        System.gc()
        SystemFinalization.runFinalizers()
        InfoDumper.upload(Crumb.GCSource, GCNative.jemallocDump("shutdown"))
      }
    }
  } catch {
    case NonFatal(t) => log.warn("Failed to flush before exit", t)
  }

  private def thisIsMainClass: Boolean = {
    val trace = Thread.currentThread().getStackTrace
    val entryPoint = trace.last
    // if this is the main class (and we're on the main thread) then the end of the stack should be ThisClass.main
    entryPoint.getClassName == getClass.getName.stripSuffix("$") && entryPoint.getMethodName == "main"
  }

  def defaultExitHandler(ex: Option[Exception], startTimeMillis: Long, retCode: Int, isComplete: Boolean): Unit = {
    // don't flush and dump loads of data if we're not the top-level main class of the app (e.g. if we're being
    // invoked within some other app or junit test) so that we don't write redundant data (potentially many times!)
    if (thisIsMainClass) {
      if (DiagnosticSettings.infoDumpOnShutdown && retCode != 0)
        InfoDumper.dump("nonZeroExitCode", msgs = List(s"retCode=$retCode"), noMore = true)
      flushBeforeExit()
    }
    exitMode match {
      case ExitMode.ForceExit => exit(ex, retCode)
      case ExitMode.Suspend =>
        if (!isComplete && ex.isEmpty) exit(ex, retCode)
        ex.foreach(_.printStackTrace)
        GraphInputConfiguration.setTraceMode(OGTraceMode.none)
        println("suspending process on OptimusApp exit")
        suspend()
      case _ => if (!isComplete || retCode != 0) exit(ex, retCode)
    }
  }

  protected override def dalLocation: DalLocation =
    DalLocation(
      if (cmdLine.env != null && cmdLine.env.nonEmpty) Some(DalEnv(cmdLine.env)) else None,
      Option(cmdLine.uri))

  protected final def establishedRoleset: Set[String] = OptimusApp.establishedRoleset

  override protected final def rolesetMode: RolesetMode =
    if (cmdLine.allRoles) RolesetMode.AllRoles
    else if (cmdLine.rolesets.isEmpty) super.rolesetMode
    else
      RolesetMode.fromSeqSetString(
        Seq(
          cmdLine.rolesets.asScala
            .collect {
              case str if str.nonEmpty => str.split(",").toSet
            }
            .flatten
            .toSet))

  protected override def zone: DalZoneId = {
    // precedence order: the jvm property > cmdLine > env var
    val cmdOrEnv =
      if (cmdLine.zone != null && cmdLine.zone.nonEmpty) cmdLine.zone
      else OptimusApp.systemEnv.getOrElse("ZONE_NAME", DalZoneId.default.underlying)
    DalZoneId(System.getProperty("ZONE_NAME", cmdOrEnv))
  }

  protected override def sessionTokenLocation: Option[String] = Option(cmdLine.sessionTokenLocation)

  protected override def appId: DalAppId = {
    // precedence order: the jvm property > cmdLine > env var
    val cmdOrEnv =
      if (cmdLine != null && cmdLine.appId != null && cmdLine.appId.nonEmpty) cmdLine.appId
      else {
        val cn = this.getClass.getName
        OptimusApp.systemEnv.getOrElse("APP_NAME", if (cn.endsWith("$")) cn.dropRight(1) else cn)
      }
    DalAppId(System.getProperty("APP_NAME", cmdOrEnv))
  }

  protected override def scopedPlugins: Map[NodeTaskInfo, ScopedSchedulerPlugin] =
    cmdLine.scopedConfiguration.map(_.scopedPlugins).orNull

  private def setSystemPropertyOverrides(): Unit = {
    // always set this property to use non kerberos connections to zk on client side
    System.setProperty("optimus.dsi.ZKConfig.disableKerberized", "true")

    if (System.getProperty("msjava.connection-reporting.enabled") eq null) {
      System.setProperty("msjava.connection-reporting.enabled", "false")
    }
  }

  protected def parseCmdline(args: Array[String], exitHandler: ExitHandler): Unit = {
    // Allow JVM options to be passed as application arguments. The `scala` or `scala.bat`
    // script will already have converted them to JVM options, but also have passes them
    // as application arguments.
    val appArgs = args.filterNot(_.startsWith("-J-"))
    this._args = appArgs
    cmdLine = manifest.runtimeClass.getDeclaredConstructor().newInstance().asInstanceOf[Args]
    parser = new CmdLineParser(cmdLine)
    try {
      parser.parseArgument(appArgs.toArray: _*)
    } catch {
      case x: CmdLineException =>
        System.err.println(x.getMessage)
        parser.printUsage(System.err)
        exitHandler(None, System.currentTimeMillis(), 1, false)
    }
  }

  private def deprecatedMatchNotifier(t: Throwable, proxy: ExceptionProxy, n: Int): Unit = {
    if (proxy.isInstanceOf[Deprecated] && n == 1) {
      val msg = t.getMessage.take(1000)
      val stack = t.getStackTrace.take(10).mkString(";")
      val newmsg = s"""Deprecated proxy matched; "$msg"; $proxy[n >= $n] """
      log.error(newmsg, t)
      Breadcrumbs.send(
        LogPropertiesCrumb(
          ChainedID.root,
          BCProps.logMsg -> newmsg
        ))
    }
  }

  // TODO (OPTIMUS-29499): should be final
  def main(args: Array[String]): Unit = {
    try {
      main(args, defaultExitHandler)
    } catch {
      case t: Throwable =>
        val expOpt = Some(new RuntimeException(t))
        defaultExitHandler(expOpt, System.currentTimeMillis(), 1, false)
    } finally {
      if (thisIsMainClass && !residentZkaContexts) {
        ZkUtils.unregisterContexts()
        OptimusCompositeLeaderElectorClient.reset()
      }
    }
  }

  // TODO (OPTIMUS-29499): should be final
  def main(args: Array[String], exitHandler: ExitHandler): Unit = {
    FileInterceptor.enableIfRequired()
    CachedExecutionPlugin.discover(useInMemoryCaching).headOption match {
      case Some(executionPlugin) =>
        executionPlugin.execute(this.getClass, args, exitHandler) match {
          case ExecutionCached =>
            // needed for defaultExitHandler
            parseCmdline(args, exitHandler)
            exitHandler(None, System.currentTimeMillis(), 0, true)
          case NoCacheFound(cachingHandler, false) =>
            execute(args, cachingHandler)
          case NoCacheFound(_, true) =>
            // needed for defaultExitHandler
            parseCmdline(args, exitHandler)
            exitHandler(None, System.currentTimeMillis(), 0, true)
          case CachingDisabled =>
            execute(args, exitHandler)
        }
      case None =>
        execute(args, exitHandler)
    }
  }

  final def execute(args: Array[String], exitHandler: ExitHandler): Unit = {
    OptimusAppTrait.currentOptimusApp = Some(this)
    val startTimeMillis = System.currentTimeMillis()
    parseCmdline(args, exitHandler)

    val OptimusStorageUnderlying(scopedMap, processMap) =
      OptimusAppTrait
        .loadCommandLine(OptimusNodeInputStorage(OptimusLoaders.defaults), cmdLine)
        .underlying
    ProcessState.setState(processMap)
    scopedInputs.set(scopedMap)

    // must be before first reported event below
    GridProfilerDefaults.setNonNodeInputDefaults(
      cmdLine.profileAggregationType,
      cmdLine.profileCustomMetrics.toArray,
      cmdLine.configFolder,
      cmdLine.pgoModes,
      cmdLine.pgoFolder
    )

    val preOptimusStartupEvent = StartupEventCounter.report("Pre Optimus Startup")

    setSystemPropertyOverrides()

    AsyncProfilerIntegration.autoStart(cmdLine.asyncProfiler)
    SamplingProfiler.applicationSetup(cmdLine.appId)

    if (ExitInterceptProp.requestInfoDump())
      SystemExitReplacement.setHook(
        "infodump",
        retCode => {
          InfoDumper.dump("intercepted", msgs = List(s"retCode=$retCode"), noMore = true)
        })

    ExceptionProxy.registerNotifier("deprecated", Some(deprecatedMatchNotifier))

    HeapSampling.initialize()

    help(exitHandler, startTimeMillis)

    OptimusLogging.install()
    LoggingHelper.mangleLoggerNames()

    StartupEventCounter.reporting("OptimusApp Setup") { setup() }

    OptimusEarlyInitializer.ensureInitialized
    InfoDumper.ensureInitialized()
    GCNative.ensureLoaded()
    if (System.getProperty("optimus.gc.disable") eq null) gc.monitor()

    // Log actual JVM params, executed class, and command line params
    // IFF this uses the standard Log trait.
    // Assumes 'setup' has initialized all desired Log settings
    this match {
      case theLog: optimus.platform.util.Log =>
        val rt = java.lang.management.ManagementFactory.getRuntimeMXBean
        val jvmArgs = rt.getInputArguments.asScala.toList
        // unmangle the name to get something human-readable
        val executed = this.getClass.getName.substring(0, this.getClass.getName.indexOf('$'))
        log.info(s"   JVM args: ${jvmArgs.mkString(" ")}")
        log.info(s"   AgentInfo: ${AgentInfo.agentInfo().asScala.mkString(", ")}")
        log.info(s"   Executing: $executed")
        log.info(s"   Program args: ${this.args.toList
            .map { str =>
              if (str.contains(" ")) s""""$str"""" else str
            }
            .mkString(" ")}")

      case _ =>
    }

    checkAsyncLogging recover { case e: Exception =>
      exitHandler(Some(e), startTimeMillis, 1, false)
    }

    /**
     * When we are running an optimus app we don't want to trigger a closedown of the DAL connection on exit of the App
     * body/main method. The problem is that an application such as a GUI, reactive, or server may do work based on some
     * other user input source, such as a UI, feed from DAL, Filter data etc.
     *
     * If a gui/reactive has a command in flight to the DAL when the closedown occurs, it will be failed. If the task
     * completed before the closedown, it would be fine, and if a task was started after the closedown occurs, then the
     * DAL connection would be re-established. Having the task behavior affected by the timing of the closedown makes
     * the task non-RT.
     *
     * If the application is terminating anyway, then the connection will die. If the connection is not terminating then
     * closing the DAL connection is not the real problem
     */
    OGTrace.startupSetupComplete(preOptimusStartupEvent)
    withOptimus(
      () => {
        try {
          OGTrace.startupOptimusInitComplete()
          preRun()
          ensureDelayedInit(onlyRunOnce = true)
          OGTrace.startupAppInitComplete()
          Runtime.getRuntime.addShutdownHook(new Thread(() => {
            if (DiagnosticSettings.infoDumpOnShutdown)
              InfoDumper.dump("shutdownHook", noMore = true)
            Breadcrumbs.flush()
            AuxiliarySchedulerHelper.flush()
          }))
          // done just before run to capture any changes in e.g. system properties due to initialization steps
          if (appLogJson) logEnvironment(this.getClass.getName)
          if (appDryRun) notActuallyRunningThisApp() else run()
        } catch {
          case e: Exception =>
            resetDelayedInit()
            exitHandler(Some(e), startTimeMillis, 1, false)
        }
      },
      Some(DalAsyncBatchingConfig.default),
      OptimusTask.KeepAlive
    )

    if (Properties.propIsSet("scala.time")) {
      val total = currentTime - OptimusApp.startupTime
      Console.println("[total " + total + "ms]")
    }

    resetDelayedInit()
    exitHandler(None, startTimeMillis, returnCode, true)
  }

  def help(exitHandler: ExitHandler, startUpTime: Long = System.currentTimeMillis()): Unit = {
    if (cmdLine.help) {
      parser.printUsage(System.out)
      exitHandler(None, startUpTime, 0, false)
    }
  }

  protected val initCode = new ListBuffer[() => Unit]
  private var delayedInitEnabled = true

  @entersGraph def run(): Unit

  // run() can be overridden so we also call this method in execute()
  // some tests call run() so we have to allow calling delayedInit multiple times in those cases
  // some tests also call main() and we allow calling multiple times by resetting the flag after the main has executed
  private[optimus] def ensureDelayedInit(onlyRunOnce: Boolean = false): Unit = {
    if (delayedInitEnabled) {
      if (onlyRunOnce) delayedInitEnabled = false
      if (appDryRun) log.error("Delayed init is not executed because optimus.app.dry.run is set to true.")
      else for (proc <- initCode) proc()
    }
  }

  // we reset the flag before the end of main() so that delayedInit can be run again for test purposes
  private[optimus] def resetDelayedInit(): Unit = delayedInitEnabled = true

  def setup(): Unit = {}
  @entersGraph def preRun(): Unit = {
    val id = ChainedID.root
    Breadcrumbs.send(OnceByCrumbEquality, id, new EventCrumb(_, RuntimeSource, Events.AppStarted))
    val truncatedOpts = LoggingInfo.getJavaOpts(2000)
    var len = 0 // (I know I can do it purely with a scanLeft but a var seems easier to read)
    val argsSeq = Option(args).map(_.toSeq).getOrElse(Seq())
    val truncatedArgs = argsSeq.takeWhile { s =>
      len += s.length
      len <= 2000
    }

    // report the profiling mode in vm options (if set) since it overrides command line
    val profilingMode = ProcessGraphInputs.ProfileLevel.currentValueOrThrow().toString
    val propertyMap = appLaunchContextInfo() ++ Seq(
      BCProps.agents -> AgentInfo.agentInfo().asScala,
      BCProps.pid -> MSProcess.getPID,
      BCProps.host -> LoggingInfo.getHost,
      BCProps.logFile -> LoggingInfo.getLogFile,
      BCProps.argsMap -> parseOptimusArgsToMap(
        if (truncatedArgs.length < argsSeq.length) truncatedArgs :+ "..." else argsSeq,
        parser),
      BCProps.event -> Events.AppStarted.name,
      BCProps.className -> this.getClass.getName,
      BCProps.appDir -> Option(System.getenv("APP_DIR")).getOrElse("unknown"),
      BCProps.javaVersion -> System.getProperty("java.version"),
      BCProps.javaHome -> System.getenv("JAVA_HOME"),
      BCProps.javaOpts -> truncatedOpts,
      BCProps.scalaHome -> System.getProperty("scala.home"),
      BCProps.osName -> System.getProperty("os.name"),
      BCProps.osVersion -> System.getProperty("os.version"),
      BCProps.sysLoc -> System.getenv("SYS_LOC"),
      BCProps.tmInstance -> treadmillInstance().getOrElse("N/A"),
      BCProps.logname -> Option(System.getenv("OPTIMUS_INFERRED_SUUER")).getOrElse("N/A"),
      BCProps.profilingMode -> profilingMode.toLowerCase,
      BCProps.withConsole -> DiagnosticSettings.diag_showConsole,
      BCProps.invocationStyle -> sys.env.getOrElse("OPTIMUS_INVOCATION_STYLE", "default"),
      BCProps.uuidLevel -> BreadcrumbLevel.asString(ChainedID.root.crumbLevel)
    )
    Breadcrumbs.send(
      OnceByCrumbEquality,
      id,
      PropertiesCrumb(
        _,
        RuntimeSource + SamplingProfilerSource,
        propertyMap
      )
    )

    ArgumentPublisher.publishArgs(argsSeq, parser)

    // Send as separate crumb to avoid size filter.  Hide behind property until we figure out how
    // to get this not to break DTC.
    if (DiagnosticSettings.getBoolProperty("optimus.app.cpuinfo", true))
      Breadcrumbs.send(
        OnceByCrumbEquality,
        id,
        PropertiesCrumb(
          _,
          RuntimeSource + SamplingProfilerSource,
          BCProps.cpuInfo -> HardwareInfo.cpuInfo,
          BCProps.memInfo -> HardwareInfo.memInfo
        )
      )

    // To track usages of this flag outside of tests
    if (Settings.allowIllegalOverrideOfInitialRuntimeEnvironment)
      Breadcrumbs.send(
        OnceByCrumbEquality,
        id,
        PropertiesCrumb(_, RuntimeSource, BCProps.overrideInitRuntimeEnv -> true))
  }
  private def appLaunchContextInfo(): BCProps.Elems = {
    val userName = System.getProperty("user.name")
    val appLaunchContextType = System.getProperty("appLaunchContext.type", "UNKNOWN")
    val appLaunchContextEnv = System.getProperty("appLaunchContext.env", "UNKNOWN")
    val appLaunchContextName = System.getProperty("appLaunchContext.name", "UNKNOWN")

    val maybeOriginalPath = Option(System.getenv("APP_ORIGINAL_PATH"))
    val maybeExpandedPath = Option(System.getenv("APP_EXPANDED_PATH"))
    val maybeNfsPrefix = Option(System.getenv("NFS_PREFIX"))
    val maybeAfsPrefix = Option(System.getenv("AFS_PREFIX"))
    val maybeAppGroupFilePath = Option(System.getenv("APP_GROUP_FILE_LOCATION"))
    val maybeGridEngineName = Option(System.getenv("GRID_ENGINE_NAME"))

    val gridEngineInfo =
      maybeGridEngineName
        .map(engine => Seq(BCProps.appGridEngineName -> engine))
        .getOrElse(Seq.empty)
    val mprInfo = (maybeNfsPrefix, maybeAfsPrefix) match {
      case (Some(nfsPrefix), Some(afsPrefix)) =>
        val originalPathInfo = maybeOriginalPath
          .flatMap(appOriginalPath =>
            InstallCommonPath(appOriginalPath, nfsPrefix, afsPrefix).metaProjectRelease.map { mpr =>
              val appGroup =
                maybeAppGroupFilePath.flatMap(appGroupFilePath =>
                  AppGroupFinder.findAppGroupFromFilePath(appGroupFilePath, mpr))
              Seq(
                BCProps.appOriginalPath -> appOriginalPath,
                BCProps.appReleaseLink -> mpr.release.getOrElse("UNKNOWN"),
                BCProps.appGroup -> appGroup.getOrElse(AppGroupFinder.notSetPlaceHolder)
              )
            })
          .getOrElse(Seq(BCProps.appOriginalPath -> "UNKNOWN"))

        val fullPathInfo = maybeExpandedPath
          .flatMap(appExpandedPath =>
            InstallCommonPath(appExpandedPath, nfsPrefix, afsPrefix).metaProjectRelease.map { mpr =>
              Seq(
                BCProps.appExpandedPath -> appExpandedPath,
                BCProps.appMeta -> mpr.meta,
                BCProps.appProject -> mpr.proj,
                BCProps.appRelease -> mpr.release.getOrElse("UNKNOWN")
              )
            })
          .getOrElse(Seq(BCProps.appExpandedPath -> "UNKNOWN"))

        originalPathInfo ++ fullPathInfo
      case _ => Seq(BCProps.appOriginalPath -> "UNKNOWN", BCProps.appExpandedPath -> "UNKNOWN")
    }
    val appInfo = mprInfo ++ gridEngineInfo

    Version.verboseProperties ++ Seq(
      BCProps.appId -> appId.underlying,
      BCProps.user -> userName,
      BCProps.appLaunchContextType -> appLaunchContextType,
      BCProps.appLaunchContextEnv -> appLaunchContextEnv,
      BCProps.appLaunchContextName -> appLaunchContextName,
      BCProps.autosysJobName -> AutoSysUtils.getAutoSysJobNameOrNone.getOrElse("none"),
      BCProps.autosysInstance -> AutoSysUtils.getAutoSysInstanceOrNone.getOrElse("none")
    ) ++ appInfo
  }

  private def checkAsyncLogging: Try[Unit] = Try {
    if (Settings.checkAsyncLogging) {
      val syncLoggers: Seq[Logger] = LoggingHelper.syncLoggers
      if (syncLoggers.nonEmpty) {
        throw new RuntimeException(
          s"Sync loggers detected: ${syncLoggers.map(_.getName).mkString("[", ",", "]")}. " +
            "Please configure async appenders for them. " +
            "To skip this check, specify system property optimus.logging.checkAsync=false"
        )
      }
    }
  }

  def logEnvironment(main: String): Unit = {
    ProcessEnvironment.logToTmp(ChainedID.root.repr, main) match {
      case Success(path)      => log.info(s"Optimus app launch context logged to ${path}")
      case Failure(exception) => log.error(s"Unable to log Optimus app launch context: ${exception.toString}")
    }

  }

  /**
   * Doesn't do anything.
   */
  @entersGraph def notActuallyRunningThisApp(): Unit = {
    log.error("App is not executed because optimus.app.dry.run is set to true.")
  }

}
object OptimusAppTrait {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  @volatile private[platform] var currentOptimusApp: Option[OptimusAppTrait[_]] = None
  def shutDownCurrentOptimusApp(t: Throwable): Unit = {
    log.warn(s"Shutting down current optimus app due to ${t.toString}!")
    currentOptimusApp.foreach(oapp =>
      oapp.defaultExitHandler(Some(new RuntimeException(t)), System.currentTimeMillis(), 1, isComplete = false))
  }

  private def lookupInput[T, V](input: NodeInput[T], value: V): Option[NodeInputResolver.Entry[_]] =
    if (value == null) None
    else {
      val registeredKey =
        Registry.TheHugeRegistryOfAllOptimusProperties.lookup(CmdLineSource.key[V](input.name)).asScala
      registeredKey.map(_.asNodeInputWithValue(value, LoaderSource.CMDLINE))
    }

  private def loadCommandLine(storage: OptimusNodeInputStorage, cmdLine: OptimusAppCmdLine): OptimusNodeInputStorage = {
    var res = storage
    val addToRes = (eo: Option[NodeInputResolver.Entry[_]]) => eo.foreach(e => { res = res.appendCmdLineInput(e) })
    Seq(
      lookupInput[JList[OptconfProvider], Seq[String]](ProcessGraphInputs.ConfigGraph, cmdLine.configGraph),
      lookupInput[OptconfProvider, String](ProcessGraphInputs.StartupConfig, cmdLine.startupConfig),
      lookupInput[String, String](ProcessGraphInputs.ProfileTSFolder, cmdLine.profileTSFolder),
      lookupInput[TemporalSurfProfilingLevel, String](ProcessGraphInputs.ProfileTSLevel, cmdLine.profileTSLevel),
      lookupInput[String, String](ProcessGraphInputs.ProfilerCsvFolder, cmdLine.profileCSVFolder),
      lookupInput[Level, String](ProcessGraphInputs.ProfileLevel, cmdLine.profileLevel),
      lookupInput[HotspotFilter, Array[String]](ProcessGraphInputs.HotspotsFilter, cmdLine.profileCustomHotspots)
    ).foreach(addToRes)
    res
  }
}

@nowarn("cat=deprecation") // DelayedInit is deprecated but so is LegacyOptimusApp (kinda)
trait OptimusAppDelayedInit[Args <: OptimusAppCmdLine] extends OptimusAppTrait[Args] with DelayedInit {
  override def delayedInit(body: => Unit): Unit = {
    initCode += (() => body)
  }
}

package magic {
  sealed class DefaultsTo[A, B]
  trait LowPriorityDefaultsTo {
    implicit def overrideDefault[A, B]: DefaultsTo[A, B] = new DefaultsTo[A, B]
  }

  object DefaultsTo extends LowPriorityDefaultsTo {
    implicit def default[B]: DefaultsTo[B, B] = new DefaultsTo[B, B]
  }
}

object OptimusApp {
  type ExitHandler = (Option[Exception], Long, Int, Boolean) => Unit

  val startupTime: Long = currentTime

  lazy val systemEnv = System.getenv().asScala

  def establishedRoleset: Set[String] = {
    // It's only possible to call this after the runtime is set up,
    // so the current session has these roles (from the cmdline or broker)
    OptimusTask.currentSessionRoles
  }
}

abstract class OptimusApp[Args <: OptimusAppCmdLine](implicit
    e: magic.DefaultsTo[Args, OptimusAppCmdLine],
    val manifest: Manifest[Args])
    extends OptimusAppTrait[Args] {
  @entersGraph override def run(): Unit
}
abstract class LegacyOptimusApp[Args <: OptimusAppCmdLine](implicit
    e: magic.DefaultsTo[Args, OptimusAppCmdLine],
    val manifest: Manifest[Args])
    extends OptimusAppDelayedInit[Args] {
  // TODO (OPTIMUS-29499): should be final
  @entersGraph override def run(): Unit = ensureDelayedInit()
}

/**
 * Configurable behaviors for when optimus is ready to end a process
 */
object ExitMode extends Enumeration {

  /** Just let the main thread die, process may hang if non-daemon threads are not cleaned up * */
  val NoAction: ExitMode.Value = Value

  /** When the main thread dies, call System.exit explicitly * */
  val ForceExit: ExitMode.Value = Value

  /** prevent the main thread from dying, generally for keeping graph debugger etc open * */
  val Suspend: ExitMode.Value = Value

  class OptionOptionHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[Option[ExitMode.Value]])
      extends OneArgumentOptionHandler[Option[ExitMode.Value]](parser, option, setter) {
    override def parse(arg: String): Option[ExitMode.Value] =
      if (!StringUtils.isBlank(arg)) { Some(ExitMode.withName(arg)) }
      else None
  }
}
