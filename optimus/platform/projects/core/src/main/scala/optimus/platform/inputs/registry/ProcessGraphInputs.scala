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
package optimus.platform.inputs.registry

import msjava.slf4jutils.scalalog.Logger
import optimus.config.EmptyOptconfProvider
import optimus.config.NodeCacheConfigs
import optimus.config.OptconfProvider
import optimus.platform.inputs.DefaultSerializableProcessSINodeInput
import optimus.platform.inputs.ProcessInputs
import optimus.platform.inputs.DefaultTransientProcessSINodeInput
import optimus.platform.inputs.EngineForwarding.Behavior
import optimus.platform.inputs.ProcessSINodeInput
import optimus.platform.inputs.StateApplicators
import optimus.platform.inputs.dist.GSFSections
import optimus.platform.inputs.registry.CombinationStrategies.graphCombinator
import optimus.platform.inputs.registry.Source.JavaProperty

import java.lang.{Boolean => JBool}
import java.util
import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.NodeTrace
import optimus.graph.cache.NCPolicy
import optimus.graph.diagnostics.gridprofiler.Filter
import optimus.graph.diagnostics.gridprofiler.FilterType
import optimus.graph.diagnostics.gridprofiler.HotspotFilter
import optimus.graph.diagnostics.tsprofiler.TemporalSurfProfilingLevel
import optimus.graph.diagnostics.tsprofiler.TemporalSurfProfilingLevel.TemporalSurfProfilingLevel
import optimus.graph.DiagnosticSettings
import optimus.graph.OGTrace.changeObserver
import optimus.graph.diagnostics.gridprofiler.Level
import optimus.graph.diagnostics.gridprofiler.Level.Level
import optimus.graph.diagnostics.trace.OGEventsObserver
import optimus.graph.diagnostics.trace.OGTraceMode
import optimus.platform.inputs.StateApplicators.StateApplicator

private[registry] object GraphInputLogger {
  val log: Logger = getLogger(this)
}

private[optimus] object ProcessGraphDefaults {
  val enableXSFTDefault: JBool = true

  private[registry] val defaultHotspotFilters: HotspotFilter = HotspotFilter(
    Filter.SELFTIME,
    FilterType.TOTAL,
    0.95
  ) // keep only the nodes whose self time contributed to the 95% of total
  // list of criteria that determine which node types to report in hotspots CSV, commandline option --profile-hotspot-filters
}

private[platform] object SharedProcessGraphInputNames {
  final val configGraph = "--config-graph"
  final val configGraphExplanation =
    "comma separated list of paths to .optconf files to be applied for this app (classpath:/ syntax is supported); the last one takes priority over conflicting configs"

  final val startupConfig = "--startup-config"
  final val startupConfigExplanation = "optconf file to be applied during startup only"

  final val profileTSLevel = "--profile-temporal-surface"
  final val profileTSLevelExplanation = "default level for profiling temporal surfaces"

  final val profileTSFolder = "--profile-ts-folder"
  final val profileTSFolderExplanation = "default folder to write temporal surfaces profiling"

  final val profileLevel = "--profile-graph"
  final val profileLevelExplanation = "default level for profiled blocks"

  final val profileHotspotfilter = "--profile-hotspot-filters"
  final val hotspotFilterExplanation = "filters that determine which hotspots are reported"
}

/**
 * should just be a class with the inputs inside. All stateApplicators and other functions should be defined in a
 * separate top level object
 */
object ProcessGraphInputs {
  import SharedProcessGraphInputNames._
  private[inputs] val FOR_TEST_ONLY_FakeTransientInput: DefaultTransientProcessSINodeInput[JBool] =
    ProcessInputs.newTransientNoSideEffects[JBool](
      "FOR_TEST_ONLY_FakeTransientInput",
      "Fake input just for testing",
      false,
      Source.fromBoolJavaProperty("optimus.dist.fakeTransientInput"),
      GSFSections.newEngineSpecJavaOpt("optimus.dist.fakeTransientInput", _.toString()),
      CombinationStrategies.graphCombinator[JBool]
    )
  private def crashIfNotEmpty(s: String): Unit = {
    if (s.isEmpty) {
      throw new IllegalStateException(s"Cannot override with nothing! Can't have empty string be valid filepath")
    }
  }

  val ProfilerCsvFolder: DefaultSerializableProcessSINodeInput[String] = ProcessInputs.newSerializableFromSources(
    "--profile-csvfolder",
    "folder for CSV output when --profile-graph is on",
    "",
    List(
      new CmdLineSource[String, String](
        "--profile-csvfolder",
        s => {
          crashIfNotEmpty(s)
          s
        }
      ),
      new JavaProperty[String](
        "optimus.scheduler.profile.folder",
        s => {
          crashIfNotEmpty(s)
          GraphInputLogger.log.warn(s"System property to override csv folder has been set to $s")
          s
        }
      )
    ),
    StateApplicators.emptyApplicator,
    Behavior.NEVER,
    CombinationStrategies.graphCombinator[String]
  )

  val ConfigGraph: DefaultSerializableProcessSINodeInput[util.List[OptconfProvider]] =
    ProcessInputs.newSerializableFromSources(
      configGraph,
      configGraphExplanation,
      util.List.of[OptconfProvider],
      List(
        new CmdLineSource[Seq[String], util.List[OptconfProvider]](
          configGraph,
          (s: Seq[String]) => s.flatMap(NodeCacheConfigs.verifyPathAndCreateProvider).asJava)
      ),
      ProcessGraphApplicators.OptconfApplicator,
      // beforehand optconf would get snapshotted in NodeCacheConfigs.confProviders which would then be put into DistClientInfo so it would effectively always get forwarded
      Behavior.ALWAYS,
      graphCombinator()
    )

  val ConfigOverride: DefaultSerializableProcessSINodeInput[util.List[OptconfProvider]] =
    ProcessInputs.newSerializableFromSources(
      "optimus.configOverride",
      "to be used only as an emergency escape hatch if we need to wire in another config file. The normal way of setting,is via --config-graph in OptimusAppCmdLine",
      util.List.of[OptconfProvider],
      List(
        new JavaProperty[util.List[OptconfProvider]](
          "optimus.configOverride",
          (s: String) => {
            if (s.isEmpty)
              throw new IllegalStateException(
                "Cannot override config with nothing! Just get rid of the application argument for --config-graph or if you cannot do that use optimus.config.ignore.")
            GraphInputLogger.log.warn(s"System property to override cache config has been set to $s")
            s.split(",").toSeq.flatMap(NodeCacheConfigs.verifyPathAndCreateProvider).asJava
          }
        )),
      ProcessGraphApplicators.OptconfApplicator,
      Behavior.ALWAYS,
      graphCombinator()
    )
  private[inputs] def parseCustomHotspotsFilter(customHotspots: Array[String]): HotspotFilter = {
    val customHotspotsA = customHotspots
    val result = customHotspotsA.length match {
      case 0 => ProcessGraphDefaults.defaultHotspotFilters // nothing to do, keep using the defaults
      case 1 if customHotspotsA(0).toLowerCase == "none" =>
        HotspotFilter(Filter.STARTS, FilterType.NONE, 0)
      case 3 =>
        HotspotFilter(
          Filter.withName(customHotspotsA(0).toUpperCase),
          FilterType.withName(customHotspotsA(1).toUpperCase),
          if (customHotspotsA(2).endsWith("%")) customHotspotsA(2).dropRight(1).toDouble / 100.0
          else customHotspotsA(2).toDouble
        )
      case _ =>
        val msg = s"""
   Invalid hotspots filter specification: filter definition must consist of three whitespace-separated tokens: metric, type, and threshold
   Supported metrics are: ${Filter.values.mkString(",")}
   Supported filter types are: ${FilterType.values.mkString(",")}
   Threshold may be specified as a regular float ("0.95") or as percentage ("95%")
   Examples: "--profile-hotspot-filters starts total 95%" will report only the hotspots whose starts counters contribute to 95% percentile of all starts
             "--profile-hotspot-filters CacheHits first 0.1" will report only the hotspots that appear in the first 10% of rows in descending cache hits order
   Proceeding with the default hotspot filter "SelfTime Total 95%"
   To request no filters, use "--profile-hotspot-filters none"
   """
        GraphInputLogger.log.warn(msg)
        ProcessGraphDefaults.defaultHotspotFilters
    }
    if (result.ratio > 1) {
      val msg =
        s"""
  Invalid hotspot filter specification $result: filter with the limit of ${result.ratio * 100}% does not filter anything"
  Proceeding with the default hotspot filter "SelfTime Total 95%"
  To request no filters, use "--profile-hotspot-filters none"
  """
      GraphInputLogger.log.warn(msg)
    }
    result
  }

  val HotspotsFilter: DefaultSerializableProcessSINodeInput[HotspotFilter] = ProcessInputs.newSerializableFromSources(
    profileHotspotfilter,
    hotspotFilterExplanation,
    ProcessGraphDefaults.defaultHotspotFilters,
    List(
      new CmdLineSource[Array[String], HotspotFilter](
        profileHotspotfilter,
        (hotspotsFilterCmdLineSrc: Array[String]) => parseCustomHotspotsFilter(hotspotsFilterCmdLineSrc)
      )
    ),
    StateApplicators.emptyApplicator,
    Behavior.ALWAYS,
    graphCombinator()
  )

  val StartupConfig: DefaultSerializableProcessSINodeInput[OptconfProvider] = ProcessInputs.newSerializableFromSources(
    startupConfig,
    startupConfigExplanation,
    EmptyOptconfProvider,
    List(
      new CmdLineSource[String, OptconfProvider](
        "--startup-config",
        (s: String) => {
          NodeCacheConfigs.verifyPathAndCreateProvider(s).getOrElse(EmptyOptconfProvider)
        })),
    ProcessGraphApplicators.OptconfApplicator,
    // this is not implemented in dist at all so it should never be sent to the engines
    Behavior.NEVER,
    graphCombinator()
  )

  val IgnoreGraphConfig: DefaultSerializableProcessSINodeInput[JBool] = ProcessInputs.newSerializableFromSources(
    "optimus.config.ignore",
    """Since RunCalc regressions run through the same launcher as normal RunCalcs, which will eventually auto-resolve to a default RunCalc optconf, we need a way to ignore the config that is applied when we run regression tests. This is because if we have a config applied that disables caching for a node, we won't track profiling data on caching for that node, and so might not generate a relevant config. Note - in the validation step, where we check that the optconf generated didn't make the regressions worse, we need to switch this off and actually run with config.""",
    false,
    List(Source.fromBoolJavaProperty("optimus.config.ignore")),
    ProcessGraphApplicators.OptconfApplicator,
    // if this is set to true then there should be no optconf applied on engine, but since optconf is autoforwarding this also needs to autoforward
    Behavior.ALWAYS,
    graphCombinator()
  )

  val EnableXSFT: DefaultSerializableProcessSINodeInput[JBool] = ProcessInputs.newSerializableFromSources(
    "optimus.profile.enableXSFT",
    "Enable XSFT caching for the process",
    ProcessGraphDefaults.enableXSFTDefault,
    List(Source.fromBoolJavaProperty("optimus.profile.enableXSFT")),
    ProcessGraphApplicators.XSFTApplicator,
    Behavior.ALWAYS,
    graphCombinator()
  )

  val ProfileTSFolder: DefaultSerializableProcessSINodeInput[String] =
    ProcessInputs.newSerializableFromSources(
      profileTSFolder,
      profileTSFolderExplanation,
      "",
      List(
        new CmdLineSource[String, String]("--profile-ts-folder", identity[String]),
        new JavaProperty[String](
          "optimus.scheduler.profile.temporalsurface.folder",
          (s: String) => {
            GraphInputLogger.log.warn(
              "Profile temporal surface folder specified as system property. Command line (if set) is ignored!")
            s
          }
        )
      ),
      StateApplicators.emptyApplicator,
      Behavior.NEVER,
      graphCombinator()
    )

  val ProfileTSLevel: DefaultSerializableProcessSINodeInput[TemporalSurfProfilingLevel] =
    ProcessInputs.newSerializableFromSources(
      profileTSLevel,
      profileTSLevelExplanation,
      TemporalSurfProfilingLevel.NONE,
      List(
        new CmdLineSource[String, TemporalSurfProfilingLevel](
          "--profile-temporal-surface",
          s => TemporalSurfProfilingLevel.withName(s.toUpperCase)),
        new JavaProperty[TemporalSurfProfilingLevel](
          "optimus.scheduler.profile.temporalsurface",
          (s: String) => {
            GraphInputLogger.log.warn(
              "Profile temporal surface level specified as system property. Command line (if set) is ignored!")
            TemporalSurfProfilingLevel.withName(s.toUpperCase)
          }
        )
      ),
      StateApplicators.emptyApplicator,
      Behavior.ALWAYS,
      graphCombinator[TemporalSurfProfilingLevel]()
    )

  val ProfileLevel: DefaultSerializableProcessSINodeInput[Level] =
    ProcessInputs.newSerializableFromSources(
      profileLevel,
      profileLevelExplanation,
      Level.NONE,
      List(
        new JavaProperty[Level](
          "optimus.scheduler.profile",
          l => {
            GraphInputLogger.log.warn(s"System property to override profiling level has been set to $l")
            if ("1" == l || "true" == l) {
              GraphInputLogger.log.error(
                s"Using $l for optimus.scheduler.profile is deprecated. Please change it to hotspots")
              Level.HOTSPOTS
            } else Level.withName(l.toUpperCase)
          }
        ),
        new CmdLineSource[String, Level](profileLevel, l => Level.withName(l.toUpperCase))
      ),
      ProcessGraphApplicators.ProfileLevelApplicator,
      Behavior.ALWAYS,
      graphCombinator()
    )
}

object ProcessGraphApplicators {
  import ProcessGraphInputs._
  val OptconfApplicator: StateApplicator = new StateApplicators.StateApplicator4[
    util.List[OptconfProvider],
    util.List[OptconfProvider],
    OptconfProvider,
    JBool] {
    override def nodeInputs: (
        ProcessSINodeInput[util.List[OptconfProvider]],
        ProcessSINodeInput[util.List[OptconfProvider]],
        ProcessSINodeInput[OptconfProvider],
        ProcessSINodeInput[JBool]) =
      (ConfigGraph, ConfigOverride, StartupConfig, IgnoreGraphConfig)

    @nowarn("msg=10500 optimus.config.NodeCacheConfigs.applyConfig")
    override private[inputs] def apply(
        optconfPaths: util.List[OptconfProvider],
        configOverride: util.List[OptconfProvider],
        startupOptconfPath: OptconfProvider,
        ignoreGraphConfig: JBool,
        firstTime: Boolean): Unit = {
      val optconfPathsToUse = if (configOverride.size == 0) optconfPaths else configOverride
      if (optconfPathsToUse.size != 0 || startupOptconfPath.nonEmpty)
        if (ignoreGraphConfig)
          GraphInputLogger.log.warn(
            s"Config file(s) ${optconfPathsToUse.asScala.mkString(",")} will be ignored since -Doptimus.config.ignore is set to true")
        else NodeCacheConfigs.applyConfig(optconfPathsToUse, startupOptconfPath)
    }

    @nowarn("msg=10500 optimus.config.NodeCacheConfigs.reset")
    override private[inputs] def resetState(): Unit = NodeCacheConfigs.reset()
  }

  val XSFTApplicator: StateApplicator = new StateApplicators.StateApplicator1[JBool] {
    override def nodeInput: ProcessSINodeInput[JBool] = EnableXSFT
    @nowarn("msg=deprecated")
    override private[inputs] def apply(enableXSFT: JBool, firstTime: Boolean): Unit =
      if (!firstTime)
        if (!enableXSFT) NodeTrace.setAllCachePoliciesTo(NCPolicy.XSFT, NCPolicy.Basic)
        else NodeTrace.setAllCachePoliciesTo(null, NCPolicy.XSFT)
    override private[inputs] def resetState(): Unit = apply(ProcessGraphDefaults.enableXSFTDefault, firstTime = false)
  }

  val ProfileLevelApplicator: StateApplicator = new StateApplicators.StateApplicator1[Level] {
    override def nodeInput: ProcessSINodeInput[Level] = ProfileLevel
    @nowarn("msg=deprecated")
    override private[inputs] def apply(lvl: Level, firstTime: Boolean): Unit = {
      val observerToUse =
        if (DiagnosticSettings.onGrid && lvl == Level.TRACENODES) {
          // we don't allow tracenodes on grid because of performance and memory concerns, not due to something inherent with tracenodes
          GraphInputLogger.log.error(
            "Cannot set Trace Nodes profiling mode on engines! Downgrading profiling to hotspots")
          OGTraceMode.hotspots
        } else {
          val name = lvl.toString
          val mode: OGEventsObserver =
            if (name == null) OGTraceMode.none
            else OGTraceMode.find(name)

          if (mode != null) mode
          else OGTraceMode.hotspots
        }
      changeObserver(observerToUse)
    }
    override private[inputs] def resetState(): Unit =
      apply(Level.NONE, firstTime = false)
  }
}
