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
package optimus.graph.tracking

import java.util.concurrent.TimeUnit

import optimus.graph.NodeDebug
import optimus.graph.NodeKey
import optimus.graph.NodeTask
import optimus.graph.Settings
import optimus.platform.Tweak

import scala.util.control.NonFatal

/**
 * Note - all calls to log* methods should be in the form {{{if (Settings.trackingScenarioLoggingEnabled) logXXX}}} this
 * enables the
 */
object DependencyTrackerLogging {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  def exceptionFree(fn: => Unit): Unit = {
    try fn
    catch {
      case NonFatal(t) =>
        log.warn("ignored", t)
      case t: Throwable =>
        log.error("unexpected", t)
        throw t
    }
  }

  def beforeRunNodes(nodes: collection.Seq[NodeTask]) =
    loggerCallback.beforeRunNodes(nodes)

  def requestUtrack(scenario: String, key: NodeKey[_]) =
    loggerCallback.requestUtrack(scenario, key)

  def logUntrack(scenario: String, key: NodeKey[_], scope: TrackingScope[_]) =
    loggerCallback.logUntrack(scenario, key, scope)

  def logTrack(scenario: String, key: NodeKey[_], scope: TrackingScope[_]) =
    loggerCallback.logTrack(scenario, key, scope)

  def logDoAddTweaks(scenario: String, tweaks: Iterable[Tweak]) =
    loggerCallback.logDoAddTweaks(scenario, tweaks)

  def logTrackUpdateMemo[M >: Null <: TrackingMemo](
      trackingScenario: String,
      utrack: TrackedNode[_],
      scope: TrackingScope[M],
      current: M,
      newMemo: M) =
    loggerCallback.logTrackUpdateMemo(trackingScenario, utrack, scope, current, newMemo)

  def callback(callbackType: String, cause: EventCause, scope: TrackingScope[_], utracks: Iterable[TrackedNode[_]]) =
    loggerCallback.callback(callbackType, cause, scope, utracks)

  def beforeRunLog(tsa: DependencyTrackerAction[_]): Unit = {
    loggerCallback.beforeRunLog(tsa)
  }
  def afterRunLog(tsa: DependencyTrackerAction[_], startNs: Long): Unit = {
    loggerCallback.afterRunLog(tsa, startNs)
  }
  def beforeRunWait(tsa: DependencyTrackerAction[_]): Long = {
    loggerCallback.beforeWaitLog(tsa)
    System.nanoTime()
  }
  def afterRunWait(
      tsa: DependencyTrackerAction[_],
      startNs: Long,
      tracker: DependencyTracker,
      trackerState: String): Unit = {
    loggerCallback.afterWaitLog(tsa, startNs, tracker, trackerState)
  }

  // deliberately a public var to allow user code to replace the logger for specialised debugging/logging
  var loggerCallback: LoggingCallback = DefaultLoggingCallback
}
object DefaultLoggingCallback extends LoggingCallback {

  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  def augmentNodeKey(nodeKey: NodeKey[_]) = nodeKey.entity match {
    case debug: NodeDebug => s"$nodeKey <<${debug.debugString}>>"
    case _                => s"$nodeKey <<${nodeKey.entity}>>"
  }
  def augment(nodeKey: TrackedNode[_]) = augmentNodeKey(nodeKey.underlying)

  override def beforeRunNodes(nodes: collection.Seq[NodeTask]): Unit = {
    nodes foreach {
      case n: NodeKey[_] if n.scenarioStack().isTrackingIndividualTweakUsage =>
        log.info(s"before run node ${augmentNodeKey(n)}")
      case n: TrackedNode[_] =>
        log.info(s"before run node ${augment(n)}")
      case n =>
        log.info(s"before run other node $n")

    }
  }
  def callback(callbackType: String, cause: EventCause, scope: TrackingScope[_], utracks: Iterable[TrackedNode[_]]) = {
    log.info(s"$callbackType callback due to $cause in $scope START")
    utracks foreach { u =>
      log.info(s"$callbackType callback due to $cause in $scope node ${augment(u)}")
    }
    log.info(s"$callbackType callback due to $cause in $scope END")
  }

  override def requestUtrack(scenario: String, key: NodeKey[_]) = {
    log.info(s"TSLOG: requestUtrack ${scenario}, key ${augmentNodeKey(key)}, ")
  }

  override def logTrackUpdateMemo[M >: Null <: TrackingMemo](
      scenario: String,
      utrack: TrackedNode[_],
      scope: TrackingScope[M],
      current: M,
      newMemo: M): Unit = {
    log.info(s"TSLOG: tracking memo change ${scenario}, ${augment(utrack)}, $scope $current -> $newMemo")
  }
  override def beforeRunLog(tsa: DependencyTrackerAction[_]): Unit = {
    log.info(s"TSLOG: start ${tsa.logSummary}")
  }
  override def afterRunLog(tsa: DependencyTrackerAction[_], startNs: Long): Unit = {
    log.info(s"TSLOG: in ${durationToMs((startNs))}ms ran ${tsa.logSummary}")
  }
  override def beforeWaitLog(tsa: DependencyTrackerAction[_]): Unit = {
    log.info(s"TSLOG: beforeWait ${tsa.logSummary}")
  }
  override def afterWaitLog(
      tsa: DependencyTrackerAction[_],
      startNs: Long,
      tracker: DependencyTracker,
      trackerState: String): Unit = {
    val duration = durationToMs(startNs)
    log.info(s"TSLOG: afterWait ${duration}ms ran ${tsa.logSummary}")
    if (duration > (Settings.trackingScenarioActionTimeLimit * 1000)) {
      log.warn(
        s"DependencyTracker action on (${tracker.scenarioReference}) waited for longer (${duration} ms) than " +
          s"time limit (${Settings.trackingScenarioActionTimeLimit * 1000} ms) to run: $tsa, $trackerState")
    }
  }
  override def logDoAddTweaks(scenario: String, tweaks: Iterable[Tweak]): Unit = {
    log.info(s"TSLOG: ts ${scenario} add tweaks size ${tweaks.size}")
    tweaks foreach { t =>
      log.info(s"TSLOG: ts ${scenario} add tweak ${t.target}")
    }
  }
  override def logUntrack(scenario: String, key: NodeKey[_], scope: TrackingScope[_]): Unit = {
    log.info(s"TSLOG: ts ${scenario} untrack ${augmentNodeKey(key)}")
  }
  override def logTrack(scenario: String, key: NodeKey[_], scope: TrackingScope[_]): Unit = {
    log.info(s"TSLOG: ts ${scenario} track ${augmentNodeKey(key)}")
  }
}
trait LoggingCallback {
  def beforeRunNodes(nodes: collection.Seq[NodeTask]): Unit

  def logDoAddTweaks(scenario: String, tweaks: Iterable[Tweak]): Unit

  def logUntrack(scenario: String, key: NodeKey[_], scope: TrackingScope[_]): Unit
  def logTrack(scenario: String, key: NodeKey[_], scope: TrackingScope[_]): Unit
  def logTrackUpdateMemo[M >: Null <: TrackingMemo](
      trackingScenario: String,
      utrack: TrackedNode[_],
      scope: TrackingScope[M],
      current: M,
      newMemo: M): Unit

  def requestUtrack(scenario: String, key: NodeKey[_]): Unit

  def callback(
      callbackType: String,
      cause: EventCause,
      scope: TrackingScope[_],
      utracks: Iterable[TrackedNode[_]]): Unit
  def beforeRunLog(tsa: DependencyTrackerAction[_]): Unit
  def afterRunLog(tsa: DependencyTrackerAction[_], startNs: Long): Unit
  def beforeWaitLog(tsa: DependencyTrackerAction[_]): Unit
  def afterWaitLog(
      tsa: DependencyTrackerAction[_],
      startNs: Long,
      tracker: DependencyTracker,
      trackerState: String): Unit
  def durationToMs(startNs: Long): Long = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs)

}
