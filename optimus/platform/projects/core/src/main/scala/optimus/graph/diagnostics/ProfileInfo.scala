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

import java.util.concurrent.ConcurrentHashMap
import java.util.{HashMap => JHashMap}
import java.util.{ArrayList => JArrayList}
import optimus.graph.DiagnosticSettings
import optimus.graph.JobNonForwardingPluginTagKey
import optimus.graph.NodeTask
import optimus.graph.OGSchedulerLostConcurrency.NonConcurrentLoopDesc
import optimus.graph.OGTrace
import optimus.graph.PropertyNode
import optimus.graph.diagnostics.pgo.Profiler
import optimus.platform.util.PrettyStringBuilder

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * When scheduler blocks (waits) this profile info is recorded
 */
final class WaitProfile(
    val start: Long,
    val end: Long,
    startChainTask: NodeTask,
    val endChainPropID: Int,
    val jstack: Exception)
    extends Serializable {

  val startChainPropID: Int =
    if (startChainTask eq null) -1
    else if (DiagnosticSettings.proxyInWaitChain) startChainTask.getProfileId
    else startChainTask.cacheUnderlyingNode.getProfileId

  def waitTime: Long = end - start

  private def format(pid: Int): String = {
    val pnti = OGTrace.trace.getCollectedPNTI(pid)
    if (pnti eq null) "n/a"
    else {
      pnti.freezeLiveInfo()
      pnti.fullName
    }
  }

  def timeOffset(that: WaitProfile): Double = (start - that.start) * 1e-6
  def waitTimeScaled: Double = waitTime * 1e-6
  def awaitedNode: String = format(startChainPropID)
  def awaitedChainEnd: String = format(endChainPropID)

  def jstackPreviewString: String = if (jstack eq null) "" else "+"
}

/** Profile information for ScenarioStack */
final class PScenarioStack {
  var constructingMethodID: Int = _
  var foundTweaks: Boolean = _
  var entryTask: NodeTask = _

  private var propertyNodes: ArrayBuffer[(PropertyNode[_], JArrayList[NodeTask])] = _
  def addViolation(propertyNode: PropertyNode[_], nodeTasks: JArrayList[NodeTask]): Unit =
    synchronized {
      if (propertyNodes == null) propertyNodes = new ArrayBuffer()
      propertyNodes += propertyNode -> nodeTasks
    }

  def hasViolations: Boolean = propertyNodes != null && propertyNodes.nonEmpty

  def writeViolations(sb: PrettyStringBuilder): Unit = if (propertyNodes != null) {
    sb.startBlock()
    propertyNodes.foreach { case (k, vs) =>
      k.writePrettyString(sb)
      sb.endln()
      sb.indent()
      vs.forEach { v =>
        sb ++= NodeName.nameAndSource(v)
        sb.endln()
      }
      sb.unIndent()
    }
    sb.endBlock()
  }

  def clearViolations(): Unit = {
    propertyNodes = null
    entryTask = null
  }

  private var runAndWaitCount: Int = _ // Counts runAndWait depth, need 0 to report first entry and its jvm stack
  def pushRunAndWaitCount(entryTask: NodeTask): Unit = synchronized {
    if (runAndWaitCount == 0)
      this.entryTask = entryTask
    runAndWaitCount += 1
  }

  def popRunAndWaitCount(): Boolean = synchronized {
    runAndWaitCount -= 1
    runAndWaitCount == 0
  }

  var givenProfile: GivenBlockProfile = _

  def this(prev: PScenarioStack) = {
    this()
    if (prev ne null) {
      this.givenProfile = prev.givenProfile
    }
  }

  def this(givenProfile: GivenBlockProfile) = {
    this()
    this.givenProfile = givenProfile
  }
}

/**
 * When enabled, SS usage, caching is recorded
 */
final class GivenBlockProfile(val location: StackTraceElement, val level: Int) extends NPTreeNode {
  val id: Int = Profiler.sstack_id.getAndIncrement()
  val children = new ConcurrentHashMap[AnyRef, GivenBlockProfile]()
  override def hasChildren: Boolean = children.size() > 0
  override def getChildren: Iterable[GivenBlockProfile] = children.values().asScala

  private val nonconstTweaks = new JHashMap[Class[_], AnyRef]()
  private var miss: Int = _ // Consider making it atomic
  private var hit: Int = _
  private var selfTime: Long = _

  def incHits(): Unit = hit += 1
  def incMisses(): Unit = miss += 1
  def getHits: Int = hit
  def getMisses: Int = miss
  def getByNameTweaks: JHashMap[Class[_], AnyRef] = nonconstTweaks
  def addByNameTweak(cls: Class[_], str: String): AnyRef = nonconstTweaks.put(cls, str)
  def getSelfTimeScaled: Double = selfTime * 1e-6
  def source: String = location.getFileName + ":" + location.getLineNumber
  override def title: String = name(false)
  def name(full: Boolean): String = {
    var className = location.getClassName
    if (full || className.indexOf("$") < 0) className + "." + location.getMethodName
    else {
      className = NodeName.cleanNodeClassName(className, '.')
      className
    }
  }
  def formattedLocation(full: Boolean): String = name(full) + "(" + source + ")"

  def reset(): Unit = {
    hit = 0
    miss = 0
    selfTime = 0
    nonconstTweaks.clear()
  }
  override def toString: String = formattedLocation(true)
}

private[optimus] class OGSchedulerNonConcurrentLoopKey extends JobNonForwardingPluginTagKey[NonConcurrentLoopDesc]
