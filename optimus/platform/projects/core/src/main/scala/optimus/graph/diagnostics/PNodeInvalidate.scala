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
package optimus.graph.diagnostics

import optimus.graph.DiagnosticSettings

import scala.collection.mutable.ArrayBuffer
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.tracking.EventCause
import optimus.graph.tracking.TrackedNode
import optimus.platform.ScenarioStack
import optimus.platform.Tweak
import optimus.ui.ScenarioReference

/**
 * This is a collector for information during an invalidation trace
 */
abstract class PNodeInvalidate extends HasTweakablePropertyInfo {
  def invalidatedNodeCount: Int
  def invalidatedNodes: Option[Iterable[NodeTaskInfo]]
  def add(nti: NodeTaskInfo): Unit
  def addUTrack(utrack: TrackedNode[_]): Unit
  def getUTracks: ArrayBuffer[TrackedNode[_]] = ArrayBuffer.empty[TrackedNode[_]]
  def copyForTweak(otherTweak: Tweak): PNodeInvalidate
  def tweak: Tweak = null
  override def property: NodeTaskInfo
  def tweakTo: String
  final def title: String = if (property ne null) property.fullName else "N/A"
  def rootEventCause: String
  def scenarioReference: ScenarioReference
}

object PNodeInvalidate {

  /**
   * Builds an appropriate PNodeInvalidate given the current settings, else returns null
   */
  def apply(tweak: Tweak, cause: EventCause, ref: ScenarioReference): PNodeInvalidate =
    if (NodeTrace.traceInvalidates.getValue) new PNodeInvalidateFullRecord(tweak, cause.root.cause, ref)
    else if (DiagnosticSettings.diag_showConsole) { // still count if diag_showConsole is on
      new PNodeInvalidateCount(if (tweak ne null) tweak.target.propertyInfo else null, cause.root.cause, ref)
    } else null
}

/**
 * Collects full record of all invalidated nodes (heavy weight)
 */
final class PNodeInvalidateFullRecord(
    override val tweak: Tweak,
    override val rootEventCause: String,
    override val scenarioReference: ScenarioReference,
    private val invalidated: ArrayBuffer[NodeTaskInfo] = ArrayBuffer[NodeTaskInfo](),
    @transient private val utracks: ArrayBuffer[TrackedNode[_]] = ArrayBuffer[TrackedNode[_]]())
    extends PNodeInvalidate
    with Serializable {
  override def invalidatedNodeCount: Int = invalidated.size
  override def invalidatedNodes: Option[Iterable[NodeTaskInfo]] = Some(invalidated)
  override def add(nti: NodeTaskInfo): Unit = { invalidated += nti }
  override def addUTrack(utrack: TrackedNode[_]): Unit = { utracks += utrack }
  override def getUTracks: ArrayBuffer[TrackedNode[_]] = utracks
  override def copyForTweak(otherTweak: Tweak): PNodeInvalidate =
    new PNodeInvalidateFullRecord(otherTweak, rootEventCause, scenarioReference, invalidated, utracks)
  override def property: NodeTaskInfo = if (tweak ne null) tweak.target.propertyInfo else null
  override def tweakTo: String = {
    if (tweak eq null) "N/A"
    else {
      if (tweak.tweakTemplate.isConstant) Debugger.underStackOf(ScenarioStack.constant) {
        s"${tweak.tweakTemplate.constantValue}"
      }
      else s"${tweak.tweakTemplate.computeGenerator.getClass.getName}"
    }
  }
}

/**
 * Collects the count of invalidated nodes (lightweight - doesn't hold a list of the nodes or the tweak)
 */
final class PNodeInvalidateCount(
    override val property: NodeTaskInfo,
    override val rootEventCause: String,
    override val scenarioReference: ScenarioReference,
    private var counter: Int = 0)
    extends PNodeInvalidate {
  override def invalidatedNodeCount: Int = counter
  override def invalidatedNodes: Option[Iterable[NodeTaskInfo]] = None
  override def add(nti: NodeTaskInfo): Unit = { counter += 1 }
  override def addUTrack(utrack: TrackedNode[_]): Unit = {} // don't count UTracks
  override def copyForTweak(otherTweak: Tweak): PNodeInvalidate =
    new PNodeInvalidateCount(otherTweak.target.propertyInfo, rootEventCause, scenarioReference, counter)
  override def tweakTo: String = "Not recorded (enable Record Invalidates mode to see)"
}
