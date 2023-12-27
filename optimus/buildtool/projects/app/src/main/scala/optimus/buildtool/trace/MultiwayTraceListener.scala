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
package optimus.buildtool.trace

import java.time.Instant

import ch.epfl.scala.bsp4j.MessageType
import optimus.breadcrumbs.crumbs.Properties
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace.MultiwayTraceListener.MultiwayTaskTrace
import optimus.platform._

import scala.collection.immutable.Seq

/** allows many trace listeners to listen as one */
object MultiwayTraceListener {
  def apply(tracers: Seq[ObtTraceListener]): ObtTraceListener = tracers match {
    case Seq(one) => one
    case many     => new MultiwayTraceListener(many.toList)
  }

  private final case class MultiwayTaskTrace(taskTraces: Seq[TaskTrace]) extends TaskTrace {
    override def reportProgress(message: String, progress: Double): Unit =
      taskTraces.foreach(_.reportProgress(message, progress))
    override def publishMessages(messages: Seq[CompilationMessage]): Unit =
      taskTraces.foreach(_.publishMessages(messages))
    override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit =
      taskTraces.foreach(_.end(success, errors, warnings, time))
    override def addToStat(obtStat: ObtStat, value: Long): Unit =
      taskTraces.foreach(_.addToStat(obtStat, value))
    override def setStat(obtStat: ObtStat, value: Long): Unit =
      taskTraces.foreach(_.setStat(obtStat, value))
    override def supportsStats: Boolean = taskTraces.exists(_.supportsStats)
  }
}

final case class MultiwayTraceListener private (traceListeners: List[ObtTraceListener]) extends ObtTraceListener {

  override def startBuild(): Unit = traceListeners.foreach(_.startBuild())
  override def reportProgress(message: String, progress: Double = -1.0): Unit =
    traceListeners.foreach(_.reportProgress(message))

  override def startTask(scopeId: ScopeId, category: CategoryTrace, time: Instant): TaskTrace =
    MultiwayTaskTrace(traceListeners.map(_.startTask(scopeId, category, time)))

  // by-name semantics are not quite what we want here
  override def setStat(stat: ObtStat, value: Long): Unit = traceListeners.foreach(_.setStat(stat, value))
  override def setProperty[T](p: Properties.EnumeratedKey[T], v: T): Unit = traceListeners.foreach(_.setProperty(p, v))
  override def addToStat(stat: ObtStat, value: Long): Unit = traceListeners.foreach(_.addToStat(stat, value))
  override def supportsStats: Boolean = traceListeners.exists(_.supportsStats)

  override def logMsg(msg: String, tpe: MessageType): Unit =
    traceListeners.foreach(_.logMsg(msg, tpe))

  @async private def throttleRecursive[T](id: ScopeId, fn: NodeFunction0[T], remaining: List[ObtTraceListener]): T =
    remaining match {
      case head :: tail =>
        head.throttleIfLowMem$NF(id)(asNode { () =>
          throttleRecursive(id, fn, tail)
        })
      case Nil => fn()
    }

  @async override def throttleIfLowMem$NF[T](id: ScopeId)(fn: NodeFunction0[T]): T =
    throttleRecursive(id, fn, traceListeners)

  override def endBuild(success: Boolean): Boolean = {
    // note that endBuild can be side-effecting so we need to call them all even if one returns false, so we cannot
    // use forall here
    // noinspection MapToBooleanContains
    !traceListeners.map(_.endBuild(success)).contains(false)
  }
}
