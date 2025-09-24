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
package optimus.graph

import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.BreadcrumbsSendLimit
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.RuntimeSource
import optimus.breadcrumbs.crumbs.EventCrumb
import optimus.breadcrumbs.crumbs.Events
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elems

import java.util.concurrent.TimeoutException
import optimus.core.CoreAPI
import optimus.platform.annotations.nodeLift
import optimus.platform.annotations.nodeSyncLift
import optimus.platform.EvaluationContext
import optimus.platform.ScenarioStack
import optimus.platform.annotations.deprecating
import optimus.platform.PluginHelpers.toNode
import optimus.platform.annotations.nodeLiftByName
import optimus.platform.util.Log

import java.util.StringJoiner
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object AuxiliarySchedulerHelper extends AuxiliarySchedulerTrait with Log {
  val logger = log
  private val fss = new ThreadLocal[ScenarioStack]
  private[graph] def fromScenarioStack = fss.get

  /**
   * Deferred evaluations will use an siRoot from this node's stack. This is useful (necessary) when launching from
   * off-graph.
   */
  def withNodeContext[U](node: NodeTask)(f: => U): U = {
    fss.set(node.scenarioStack())
    val ret = f
    fss.remove()
    ret
  }
}
trait AuxiliarySchedulerTrait { self =>
  import AuxiliarySchedulerHelper.logger

  /**
   * Snapshot helper methods
   */
  private[optimus] def runOnAuxiliaryScheduler[T](
      expr: () => T,
      callback: Try[T] => Unit,
      scenarioStack: ScenarioStack): Unit = {
    val n = CoreAPI.nodify(expr())
    n.attach(scenarioStack)
    AuxiliaryScheduler.runOnAuxScheduler(n, callback)
  }

  @deprecating(suggestion = "Used to replace Peripherals for async DAL action in Tracker")
  def runOnAuxSchedulerInSIScenarioStack[T](expr: () => T): Unit = {
    val n = CoreAPI.nodify(expr())
    n.attach(EvaluationContext.scenarioStack.siRoot)
    AuxiliaryScheduler.runOnAuxScheduler(n)
  }

  /**
   * Schedule f on the auxiliary thread, and forget about it. If it throws, there will be a log message, but the process
   * is permitted to exit without completing f.
   */
  def launchAndForgetSI(@nodeLift @nodeLiftByName f: => Unit): Unit = launchAndForgetSI$withNode(toNode(f _))
  def launchAndForgetSI$withNode(f: Node[Unit]): Unit = launchAndMaybeForgetSI$withNode(f, true)

  /**
   * Schedule f on the auxiliary thread and do not forget about it. Under normal conditions, optimus clients and engines
   * will wait __indefinitely__ for all such tasks to complete before exiting.
   */
  @nodeSyncLift
  def launchAndCompleteBeforeExitingSI(@nodeLift @nodeLiftByName f: => Unit): Unit =
    launchAndCompleteBeforeExitingSI$withNode(toNode(f _))
  def launchAndCompleteBeforeExitingSI$withNode(f: Node[Unit]): Unit = launchAndMaybeForgetSI$withNode(f, false)

  private val inflight = mutable.Set.empty[Node[Unit]]

  private def launchAndMaybeForgetSI$withNode(f: Node[Unit], forget: Boolean): Unit = {
    logger.debug(s"Launching $f")
    var ss: ScenarioStack = null
    val cc = OGSchedulerContext.current()
    if (cc ne null)
      ss = cc.scenarioStack()
    if (ss eq null)
      ss = AuxiliarySchedulerHelper.fromScenarioStack
    if (ss eq null)
      throw new IllegalStateException("Cannot find SI stack")
    f.attach(ss.siRoot)
    if (!forget) self.synchronized {
      inflight += f
    }
    AuxiliaryScheduler.runOnAuxScheduler(
      f,
      { result: Try[Unit] =>
        if (!forget) self.synchronized {
          inflight -= f
          self.notifyAll()
        }
        result match {
          case Success(_) =>
          case Failure(e) =>
            logger.error(s"Deferred task failed", e)
        }
      }
    )
    logger.debug(s"Launched $f")
  }
  private val MILLION: Long = 1000L * 1000L
  def flush(): Unit = self.synchronized {
    val t = System.nanoTime()
    while (inflight.nonEmpty) {
      // log every 30 seconds
      val inFlightNodes = inflight.map(_.toPrettyName(true, true)).toSeq
      if (Breadcrumbs.collecting) {
        Breadcrumbs.info(
          ChainedID.root,
          PropertiesCrumb(
            _,
            RuntimeSource,
            Elems(
              Properties.Elem(Properties.auxSchedulerTimedOutNodes, inFlightNodes),
              Properties.Elem(Properties.auxSchedulerFlushDurationNs, System.nanoTime - t))
          )
        )
      }
      val sb = new StringJoiner("\n")
      sb.add("[")
      inFlightNodes.foreach(f => sb.add("\t" + f + ","))
      sb.add("]")
      logger.warn(s"Timed out while ${inflight.size} fire-and-forget nodes were still running:" + sb.toString)
      wait(30000)
    }
    if (Breadcrumbs.collecting) {
      Breadcrumbs.info(
        ChainedID.root,
        PropertiesCrumb(
          _,
          RuntimeSource,
          Elems(Properties.Elem(Properties.auxSchedulerTotalFlushDurationNs, System.nanoTime - t)))
      )
    }
  }

}
