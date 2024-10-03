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
package optimus.core

import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.BreadcrumbsSendLimit._
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.Source
import optimus.breadcrumbs.crumbs.Events.EventVal
import optimus.breadcrumbs.crumbs.{Properties => P}
import optimus.breadcrumbs.crumbs.Properties._
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.debug.InstrumentationConfig
import optimus.graph.DiagnosticSettings
import optimus.graph.Exceptions
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.graph.diagnostics.rtverifier.Violation
import optimus.logging.LoggingInfo
import optimus.platform.EvaluationContext
import optimus.platform.ForwardingPluginTagKey
import optimus.platform.RuntimeEnvironment.getAppId
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery
import optimus.platform.temporalSurface.operations.TemporalSurfaceQueryWithClass
import optimus.platform.util.{Version => V}
import optimus.ui.ScenarioReference
import optimus.utils.MiscUtils.NullCandy._
import optimus.utils.MacroUtils.SourceLocation

object GraphDiagnosticsSource extends Source {
  override val name = "GD"
  override val maxCrumbs: Int = System.getProperty("optimus.graph.max.monitoring.crumbs", "100").toInt
}

/** Helpers for monitoring uses of APIs once per app ID per JVM */
private object MonitoringBreadcrumbsPublisher {
  // These imports are not visible in the MonitoringBreadcrumbs objects itself
  import optimus.breadcrumbs.Breadcrumbs
  private val log = getLogger(MonitoringBreadcrumbs.getClass)
  private val source = GraphDiagnosticsSource

  def note(key: LimitByKey, elems: Elems): Unit = publish("Info", key, ChainedID.root, elems)
  def note(key: LimitByKey, task: NodeTask, elems: Elems): Unit = publish("Info", key, ChainedNodeID(task), elems)

  def warn(key: LimitByKey, elems: Elems): Unit = publish("Warn", key, ChainedID.root, elems)
  def warn(key: LimitByKey, task: NodeTask, elems: Elems): Unit = publish("Warn", key, ChainedNodeID(task), elems)
  private def publish(severity: String, key: LimitByKey, id: ChainedID, elems: Elems): Unit = {
    val sent = Breadcrumbs.info(key, id, PropertiesCrumb(_, source, P.severity -> severity :: elems))
    if (sent) log.info(s"Published $elems")
    else log.debug(s"Did not publish $elems")
  }
}

object MonitoringBreadcrumbs {
  import MonitoringBreadcrumbsPublisher._
  // Make it slightly harder to call Breadcrumbs methods directly by mistake
  val Breadcrumbs = "do not call directly"

  private def toPublishLoc(sl: SourceLocation) = PublishLocation(sl.sourceName, sl.line, -1)

  def OnceByAppId(ntsk: NodeTask): OnceByKey = {
    OnceBy("appId", getAppId(ntsk).getOrElse("unknown"))
  }

  // TODO (OPTIMUS-54358): JVM uptime is fine for now, but needs consideration once treadmill hosts are pre-started
  def sendStartupTimeCrumb(eventName: String): Unit = {
    note(OnceBySourceLoc, P.event -> eventName :: jvmUptime -> DiagnosticSettings.jvmUpTimeInMs :: Elems.Nil)
  }

  // For snapping current time of an event
  // [SEE_BREADCRUMB_FILTERING]: these are wrapped in PropertiesCrumb because of the confusing filtering that is
  // configured in various breadcrumbs config json files (in particular, RedirectNonSplunkableCrumbsToDevNull, which
  // seems to disable publishing of some EventCrumbs)
  def sendMinigridEventCrumb(calcName: String, event: EventVal): Unit =
    note(OnceByCrumbEquality, P.event -> event.name :: miniGridCalc -> calcName :: Elems.Nil)

  /**
   * Send crumbs when users turn on the async stack traces feature.
   */
  private[optimus] def sendAsyncStackTracesCrumb(): Unit =
    note(OnceBySourceLoc, feature -> "AsyncStackTraces" :: V.verboseProperties)

  private[optimus] def sendXSFTCycleRecoveryCrumb(task: NodeTask, stack: String): Unit =
    warn(
      OnceBy(task.executionInfo.fullName) && OnceByAppId(task),
      task,
      xsftCycle -> task.executionInfo.fullName :: xsftStack -> stack :: Elems.Nil)

  private[optimus] def cacheTimeMisreported(task: NodeTask, time: Long): Unit = {
    val event = "CacheTimeMisreported"
    val taskName = task.executionInfo.fullName
    warn(
      OnceBy(event, taskName) && OnceByAppId(task),
      task,
      P.event -> event :: P.cacheTimeMisreported -> taskName :: profCacheTime -> time :: V.shortProperties)
  }

  private[optimus] def sendRTViolationCrumb(violation: Violation): Unit =
    note(
      4 * OnceBy(violation.key) thenBackoff, // send 4, then every power of 2
      P.user -> LoggingInfo.getUser ::
        P.event -> violation.category ::
        rtvViolation -> violation.category ::
        rtvOwner -> violation.owner ::
        rtvLocation -> violation.key ::
        rtvStack -> violation.details ::
        Elems.Nil
    )

  // send one crumb per concrete class and count
  private[optimus] def sendInstrumentedStartsCrumb(task: NodeTask, concreteClassToCount: Map[String, Long]): Unit = {
    val event = "InstrumentedConcreteNodeStarts"
    val base = InstrumentationConfig.instrumentedBaseClass
    concreteClassToCount.foreach { case (concrete, count) =>
      note(
        OnceByCrumbEquality,
        task,
        P.event -> event :: baseClass -> base :: concreteClass -> concrete :: profStarts -> count :: Elems.Nil)
    }
  }

  /** send host and path info when file-backed storage is out of file space.. */
  private[optimus] def sendOGTraceStoreProblemCrumb(path: String, exc: String): Unit =
    warn(
      OnceByCrumbEquality,
      P.event -> "ogtraceBackingStoreProblem" :: P.file -> path :: P.ogtraceBackingStoreProblem -> exc :: V.shortProperties)

  /**
   * For monitoring how many non-trivial uses of givenOverlay there are per app ID. Consider migrating all trivial uses
   * of givenOverlay to evaluateIn (as long as ScenarioReference.current is not expected to refer to the 'corrected'
   * scenario -- [SEE_CURRENT_SCENARIO_REF])
   */
  private val sentOverlayCrumb = new IntOpenHashSet

  private[optimus] def sendGivenOverlayCrumb(current: ScenarioReference, overlay: ScenarioReference): Unit =
    // check triviality once per current+overlay pair per root ID
    if (sentOverlayCrumb.synchronized(sentOverlayCrumb.add((ChainedID.root, current, overlay).hashCode))) {
      val trivial = current.isSelfOrParentOf(overlay)
      if (trivial)
        log.warn(s"${current.name}.givenOverlay(${overlay.name}) is trivial, consider evaluateIn API instead")
      val props = Elems(
        currentScenario -> current.name,
        overlayScenario -> overlay.name,
        trivialOverlay -> trivial,
        event -> "givenOverlay")
      // Publish at most 10 of these, then exp backoff
      warn(10 * OnceBySourceLoc thenBackoff, props)
    }

  private val sentScenarioReferenceCrumb = new IntOpenHashSet

  /** only if currently in givenOverlay */
  private[optimus] def sendCurrentScenarioReferenceCrumb(): Unit = {
    val currentNode = EvaluationContext.currentNode
    val inOverlay = currentNode.scenarioStack.findPluginTag(InGivenOverlayKey)
    if (inOverlay.nonEmpty) {
      val rootID = ChainedID.root
      val waiters = currentNode.waitersToFullMultilineNodeStack
      // send crumb at most once per use of ScenarioReference.current in givenOverlay per root ID per node stack
      if (sentScenarioReferenceCrumb.synchronized(sentScenarioReferenceCrumb.add((rootID, waiters).hashCode))) {
        warn(
          5 * OnceBySourceLoc thenBackoff,
          P.event -> "CurrentScenarioReferenceInOverlay" ::
            logMsg -> s"Used ScenarioReference.current API in givenOverlay: \n$waiters" :: Elems.Nil
        )
      }
    }
  }

  private[optimus] object InGivenOverlayKey extends ForwardingPluginTagKey[AnyRef]
  private[optimus] object InGivenOverlayTag

  private def exceptionToReport(exception: Throwable): Throwable =
    if (exception ne null) exception else new Exception("GraphFatalException")

  private[optimus] def sendGraphFatalErrorCrumb(reason: String): Unit = {
    warn(OnceBy(reason), P.event -> "setGraphFatalError" :: P.reason -> reason :: V.properties)
  }

  private[optimus] def sendGraphFatalErrorCrumb(
      reason: String,
      ntsk: NodeTask,
      exception: Throwable,
      logFile: String,
      logMsg: String): Unit = {

    val stackTrace = exception.nonNull(e => Exceptions.minimizeTrace(exceptionToReport(e), 10, 3))

    val nti: Option[String] = ntsk.nonNull(_.info).map(_.toString)
    val pluginName: Option[String] =
      (ntsk.nonNull(_.getReportingPluginType) orElse (ntsk.nonNull &? (_.getPlugin) &? (_.pluginType))).map(_.toString)
    warn(
      OnceBySourceLoc,
      P.event -> "fatalGraphEx" ::
        P.reason -> reason ::
        P.node.maybe(nti) ::
        Option(ntsk).map(P.nodeStack -> _.simpleChain()) ::
        P.plugin.maybe(pluginName) ::
        P.stackTrace.maybe(stackTrace) ::
        P.exception.nonNull(exception) ::
        P.logMsg.nonNull(logMsg) ::
        P.logFile.nonNull(logFile) ::
        V.properties
    )
  }

  private[optimus] def sendDalAccessWithTickingContextCrumb(query: TemporalSurfaceQuery): Unit = {
    val event = "dalAccessWithTickingContext"
    val queryType = query.getClass.getSimpleName
    val targetClass = query match {
      case c: TemporalSurfaceQueryWithClass => c.targetClass.getName
      case _                                => "(unknown)"
    }
    warn(
      OnceBy((event, targetClass, queryType)),
      P.event -> event ::
        P.tsQueryClassName -> targetClass ::
        P.tsQueryType -> queryType ::
        V.properties)
  }

  private[optimus] def sendTrackingTemporalContextInvalidAssertion(): Unit = {
    sendWithStackAsProperties(OnceBySourceLoc, "temporalContextTrackingFailedAssertion", EvaluationContext.currentNode)
  }

  private[optimus] def sendTrackingTemporalContextTurnedOn(loc: SourceLocation): Unit = {
    val key = OnceBySourceLoc(toPublishLoc(loc)) // we publish once per source location *for the reactive binding*

    val nodeInfos = for {
      node <- Option(OGSchedulerContext.current()).map(_.getCurrentNodeTask)
      info <- Option(node.info)
    } yield P.node -> info.toString ::
      P.nodeStack -> node.waitersToNodeStack(false, true, false, -1) ::
      Elems.Nil

    val props: Elems = P.event -> "temporalContextNeedsTracking" ::
      P.file -> loc.sourceName ::
      P.methodName -> loc.method ::
      P.line -> loc.line ::
      P.stackTrace -> Thread.currentThread().getStackTrace.map(_.toString) ::
      nodeInfos.getOrElse(Elems.Nil) ::: V.properties
    warn(key, props)
  }

  private[optimus] def sendBadSchedulerStateCrumb(schedulerState: String): Unit =
    warn(OnceByCrumbEquality, P.event -> "BadSchedulerState" :: P.schedulerState -> schedulerState :: V.properties)

  private[optimus] def sendEvilCallFromSINodeToGivenRuntimeEnvNode(): Unit =
    sendWithStackAsProperties(OnceBySourceLoc, "EvilCallFromSINodeToGivenRuntimeEnvNode", EvaluationContext.currentNode)

  private[optimus] def sendEvilScenarioStackInitTimeRandom(expected: String, initial: String): Unit = {
    val event = "EvilScenarioStackInitTimeRandomV5"
    val props = Elems(P.event -> event, scenarioExpected -> expected, scenarioFound -> initial) + V.properties
    warn(OnceBy(event), props)
  }

  private[optimus] def sendTweaksConflictingWithScenarioUnordered(ex: Throwable): Unit = {
    val event = "TweaksConflictingWithScenarioUnordered"
    sendWithStackAsProperties(10 * OnceBySourceLoc thenBackoff, event, ex.getStackTrace, EvaluationContext.currentNode)
  }

  /** name = module name looked up, cls = name of the class that did the lookup */
  private[optimus] def sendScalaReflectionFallbackCrumb(name: String, cls: String, event: String): Unit = {
    val props = Elems(P.event -> event, scalaFallbackModule -> name, scalaFallbackCls -> cls) + V.properties
    warn(5 * OnceBySourceLoc thenBackoff, props)
  }

  private[optimus] def sendRTVerifierTriggerFailure(ex: Throwable): Unit = {
    val event = "RTVerifierTriggerFailure"
    val props = Elems(P.event -> event, stackTrace -> ex.getStackTrace.map(_.toString)) + V.properties
    warn(OnceBy(event), props)
  }

  /** send a breadcrumb when a batcher batches an XS node */
  private[optimus] def sendBatchingInsideXSCallStackCrumb(node: NodeTask): Unit = synchronized {
    sendWithStackAsProperties(OnceBySourceLoc, "batchingInsideXSCallStack", node)
  }

  def sendCriticalSyncStackCrumb(nodeStackTrace: String, count: Int): Unit =
    warn(
      OnceByCrumbEquality,
      Elems(P.event -> "criticalSyncStack", countNum -> count, P.nodeStack -> nodeStackTrace) ::: V.properties)

  private def sendWithStackAsProperties(key: LimitByKey, eventName: String, node: NodeTask): Unit =
    sendWithStackAsProperties(key, eventName, Thread.currentThread().getStackTrace, node)

  private def sendWithStackAsProperties(
      key: LimitByKey,
      eventName: String,
      stackTraceElements: Array[StackTraceElement],
      node: NodeTask): Unit = {
    val props = P.event -> eventName ::
      P.node -> Option(node.info).map(_.toString).getOrElse("") ::
      P.stackTrace -> stackTraceElements.map(_.toString) ::
      P.nodeStack -> node.waitersToNodeStack(false, true, false, -1) ::
      V.properties
    warn(key, props)
  }
}
