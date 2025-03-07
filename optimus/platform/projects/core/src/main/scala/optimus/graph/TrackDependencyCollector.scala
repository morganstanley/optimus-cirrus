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

import optimus.platform.EvaluationQueue
import optimus.platform.NodeHash
import optimus.platform.PluginHelpers.toNodeFactory
import optimus.platform.annotations.nodeLift
import optimus.platform.annotations.nodeSyncLift

import java.util.concurrent.ConcurrentHashMap
import scala.collection.Set
import scala.collection.immutable
import scala.jdk.CollectionConverters._

object TrackDependencyCollector {
  def allProperties: TrackDependencyCollector[Set[NodeTaskInfo]] = new TrackDependencyOnPropertiesCollector()

  @nodeSyncLift
  def filteredAndKeyed[K](@nodeLift f: PropertyNode[_] => (K, Boolean)): TrackDependencyCollector[Set[K]] =
    filteredAndKeyed$withNode(toNodeFactory(f))

  def filteredAndKeyed$withNode[K](f: PropertyNode[_] => Node[(K, Boolean)]): TrackDependencyCollector[Set[K]] =
    new TrackDependencyFilteredAndKeyed(f)
}

/**
 * The reason this class is sealed, is because it's actually quite tightly integrates with ScenarioStacks and graph
 * internals. Despite appearing trivial and nicely abstracted.
 */
sealed abstract class TrackDependencyCollector[T] {

  def result: T

  private[optimus /*platform*/ ] def setTrackingNode(node: TDNode[_, _]): Unit = {}
  private[optimus /*platform*/ ] def onResolvedNode(child: NodeTask): Unit = {}

  private[optimus /*platform*/ ] def trackTweakableHash(key: NodeHash, node: NodeTask): Unit
  private[optimus /*platform*/ ] def trackTweakable(key: PropertyNode[_], node: NodeTask): Unit
  private[optimus /*platform*/ ] def trackTweak(ttn: TweakTreeNode, node: NodeTask): Unit

}

/**
 * TrackDependencyCollector implementation that records all NodeTaskInfos it has seen
 */
final private[optimus /*platform*/ ] class TrackDependencyOnPropertiesCollector
    extends TrackDependencyCollector[Set[NodeTaskInfo]] {
  val recordedNTI = new ConcurrentHashMap[NodeTaskInfo, NodeTaskInfo]()

  override def result: Set[NodeTaskInfo] = recordedNTI.keySet().asScala: Set[NodeTaskInfo]

  private[optimus /*platform*/ ] def trackTweakableHash(key: NodeHash, node: NodeTask): Unit = {
    recordedNTI.putIfAbsent(key.propertyInfo, key.propertyInfo)
  }
  private[optimus /*platform*/ ] def trackTweakable(key: PropertyNode[_], node: NodeTask): Unit = {
    recordedNTI.putIfAbsent(key.propertyInfo, key.propertyInfo)
  }

  private[optimus /*platform*/ ] def trackTweak(ttn: TweakTreeNode, node: NodeTask): Unit = {
    recordedNTI.putIfAbsent(ttn.key.propertyInfo, ttn.key.propertyInfo)
  }
}

final private[optimus /*platform*/ ] class TrackDependencyFilteredAndKeyed[K](f: PropertyNode[_] => Node[(K, Boolean)])
    extends TrackDependencyCollector[Set[K]] {
  val recordedKeys = new ConcurrentHashMap[K, K]()
  override def result: Set[K] = recordedKeys.keySet().asScala: Set[K]

  var tdnode: TDNode[_, _] = _
  override private[optimus /*platform*/ ] def setTrackingNode(tdnode: TDNode[_, _]): Unit = this.tdnode = tdnode

  override private[optimus /*platform*/ ] def onResolvedNode(child: NodeTask): Unit = {
    val (key, reg) =
      child.asInstanceOf[Node[(K, Boolean)]].result // Combine info was already called by the called of onResolvedNode
    if (reg)
      recordedKeys.putIfAbsent(key, key)
  }

  override private[optimus /*platform*/ ] def trackTweakableHash(key: NodeHash, node: NodeTask): Unit = {
    throw new GraphException("Full tweakable information needs to be recorded")
  }

  override private[optimus /*platform*/ ] def trackTweakable(key: PropertyNode[_], node: NodeTask): Unit = {
    tdnode.scheduleResolveNode(f(key))
  }

  override private[optimus /*platform*/ ] def trackTweak(ttn: TweakTreeNode, node: NodeTask): Unit = {
    tdnode.scheduleResolveNode(f(ttn.key))
  }
}

/** Node class that represents the computation and computed dependency summary */
final private[optimus /*platform*/ ] class TDNode[T, K](c: TrackDependencyCollector[K], f: Node[T])
    extends CompletableNodeM[(T, K)] {
  private[this] val lock = new Object()
  private[this] var waitCount = 1 // Start with 1 to account for value/result node 'f'
  c.setTrackingNode(this)

  def scheduleResolveNode(filterOrKeyCompute: Node[_]): Unit = {
    lock.synchronized(waitCount += 1)
    filterOrKeyCompute.attach(scenarioStack())
    val scheduler = OGSchedulerContext.current().scheduler()
    scheduler.enqueue(filterOrKeyCompute)
    filterOrKeyCompute.continueWith(this, scheduler)
  }

  override def run(ec: OGSchedulerContext): Unit = {
    f.attach(scenarioStack.withTrackDependencies(c))
    ec.enqueue(f)
    f.continueWith(this, ec)
  }

  override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    // Important to first process the result of the filter node
    if (child ne f) {
      c.onResolvedNode(child)
    }

    val done = lock.synchronized {
      // Normally we don't need a lock for combineInfo because only one child can call back, but here
      // multiple resolve nodes and the value compute node can raise onChildCompleted
      combineInfo(child, eq) // As part of combining we copied child's exception and warnings
      waitCount -= 1
      waitCount == 0
    }

    if (done) {
      // Basically the same as completeFromNode (but we already combineInfo above)
      if (f.isDoneWithResult) completeWithResult((f.result, c.result), eq)
      else completeWithException(f.exception(), eq)
    }
  }
}

class CollectingTweakableListener(
    orgTracker: TweakableListener,
    trackingDepth: Int,
    collector: TrackDependencyCollector[_])
    extends TweakableListener
    with Serializable {
  override val respondsToInvalidationsFrom: immutable.Set[TweakableListener] = orgTracker.respondsToInvalidationsFrom
  override def isDisposed: Boolean = orgTracker.isDisposed

  /** Need to be "transparent" */
  override def onTweakableNodeCompleted(node: PropertyNode[_]): Unit = orgTracker.onTweakableNodeCompleted(node)

  /** Need to be "transparent" */
  override def onTweakableNodeHashUsedBy(key: NodeHash, node: NodeTask): Unit = {
    collector.trackTweakableHash(key, node)
    orgTracker.onTweakableNodeHashUsedBy(key, node)
  }

  /** Need to be "transparent" */
  override def onTweakableNodeUsedBy(key: PropertyNode[_], node: NodeTask): Unit = {
    collector.trackTweakable(key, node)
    orgTracker.onTweakableNodeUsedBy(key, node)
  }

  /** Need to be "transparent" */
  override def onTweakUsedBy(ttn: TweakTreeNode, node: NodeTask): Unit = {
    if (ttn.trackingDepth <= trackingDepth) collector.trackTweak(ttn, node)
    // Consider adding support for nested tweaks! [SEE_ON_TWEAK_USED]
    orgTracker.onTweakUsedBy(ttn, node)
  }

  override def reportTweakableUseBy(node: NodeTask): Unit = {}

  /** Currently there is no point of serializing this collector and it should become 'transparent' */
  protected def writeReplace(): AnyRef = NoOpTweakableListener
}
