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

import optimus.breadcrumbs.crumbs.RequestsStallInfo
import optimus.core.MonitoringBreadcrumbs
import optimus.core.NodeInfoAppender
import optimus.core.StallInfoAppender
import optimus.platform.EvaluationQueue

import java.lang.ref.WeakReference
import java.util
import java.util.Objects
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.util.Random

// private constructor to avoid ridiculous values of id
final class PluginType private (val name: String, @transient private var id: Int) extends Serializable {

  def getId: Int = id

  override def toString: String = s"PluginType($name,$id)"

  val pluginFullWaitPtr: Long = AsyncProfilerGraphIntegration.fullWaitEvent.eventPtr(name)

  def reportingInfo: NodeTaskInfo = {
    val info = NodeTaskInfo.internal(name)
    info.setReportingPluginType(this)
  }

  def recordLaunch(eq: EvaluationQueue, ntsk: NodeTask): Unit = if (DiagnosticSettings.pluginCounts) {
    OGLocalTables.borrow(eq, { lt: OGLocalTables => recordLaunch(lt, ntsk) })
  }

  def recordLaunch(lt: OGLocalTables, ntsk: NodeTask): Unit = if (DiagnosticSettings.pluginCounts) {
    val tracker = lt.pluginTracker
    // Should be uncontentended except when sampling is occurring
    tracker.synchronized {
      // starts only increases, so you can calculate the number of starts in a time range
      tracker.starts.update(this, 1)
      tracker.adaptedNodes.track(this, ntsk)
    }
  }

  def recordLaunch(ntsk: NodeTask): Unit = if (DiagnosticSettings.pluginCounts) {
    OGLocalTables.borrow(recordLaunch(_, ntsk))
  }

  def decrementInFlightTaskCount(eq: EvaluationQueue): Unit = if (DiagnosticSettings.pluginCounts) {
    OGLocalTables.borrow(eq, { lt: OGLocalTables => lt.pluginTracker.completed.update(this, 1) })
  }

  def recordFullWait(ec: OGSchedulerContext, ntsk: NodeTask, ns: Long): Unit = {
    val lt = OGLocalTables.getOrAcquire(ec)
    lt.pluginTracker.fullWaitTimes.update(this, ns)
    if (DiagnosticSettings.awaitStacks)
      AsyncProfilerGraphIntegration.fullWaitEvent.record(ec, ntsk, ns, pluginFullWaitPtr)
    lt.release()
  }

  override def equals(obj: Any): Boolean = obj.isInstanceOf[PluginType] && obj.asInstanceOf[PluginType].id == id
  override def hashCode(): Int = getId

  def readResolve(): AnyRef = PluginType.apply(name)
}

object PluginType {

  final class GuaranteedSnapped[T](val getSafe: T) extends AnyVal
  def guaranteeSnapped[T](t: T): GuaranteedSnapped[T] = new GuaranteedSnapped[T](t)
  def nonNullSnapped(g: GuaranteedSnapped[_]): Boolean = {
    Objects.nonNull(g) && Objects.nonNull(g.getSafe)
  }

  val unknownReasonStallMsg = "waiting for an unknown reason"
  private val nextId = new AtomicInteger(0)
  // Simulate innards of enum, so we can add entries on the fly
  private val vmap = mutable.HashMap.empty[String, PluginType]
  private val vs = mutable.ArrayBuffer.empty[PluginType]
  // Register a new plugin type from anywhere in the codetree, keeping names unique.
  def apply(name: String): PluginType = this.synchronized {
    vmap.getOrElseUpdate(
      name, {
        val v = new PluginType(name, nextId.getAndIncrement())
        vs += v
        v
      })
  }
  def maxId: Int = nextId.get()

  // Core plugin types are defined here.  Business-specific plugins should be defined within their own
  // packages using the same apply method.
  val DAL: PluginType = apply("DAL")
  val Dist: PluginType = apply("GSF")
  val Elevated: PluginType = apply("EGSF")
  val DMC: PluginType = apply("DMC")
  val DMC2: PluginType = apply("BatchedDmc2")
  val JOB: PluginType = apply("JOB")
  val Other: PluginType = apply("Other")
  val Diag: PluginType = apply("DIAG")
  val None: PluginType = apply("None") // No plugin installed

  class Counter {

    private[PluginType] def this(counts: Array[Long]) = {
      this()
      this.counts = counts
    }
    // one element for each plugin type
    private var counts: Array[Long] = new Array(PluginType.maxId * 2)
    def size: Int = counts.length

    // called under lock
    private def ensureAlloc(id: Int): Int = {
      if (counts.length <= id) {
        val newLength = Math.max(PluginType.maxId, id) * 2
        counts = util.Arrays.copyOf(counts, newLength)
      }
      size
    }
    def update(rp: PluginType, d: Long): Unit = this.synchronized {
      ensureAlloc(rp.id)
      counts(rp.id) += d
    }

    def snapArray(): GuaranteedSnapped[Array[Long]] = this.synchronized {
      guaranteeSnapped(util.Arrays.copyOf(counts, counts.length))
    }

    def snap(): GuaranteedSnapped[Counter] = {
      val c = new Counter
      c.counts = snapArray().getSafe
      guaranteeSnapped(c)
    }

    def diff(pc: Counter): Counter = {
      val c = snap().getSafe
      c.accumulate(pc, -1)
      c
    }

    def accumulate(pc: Counter, mult: Long = 1): Counter = this.synchronized {
      val c = pc.snapArray().getSafe
      ensureAlloc(c.length - 1)
      for (i <- c.indices) counts(i) += c(i) * mult
      this
    }

    def toMap: Map[String, Long] = this.synchronized {
      val vv = for (i <- counts.indices if counts(i) != 0) yield vs(i).name -> counts(i)
      vv.toMap
    }

    override def toString: String = toMap.toString
  }

  private val forceCompactFloor = 100
  private val minNodeArraySize = 4

  // Not using NodeInfoAppender, because we need to know plugin type after completion
  private val pluginTypes = NodeInfoAppender.accessor[PluginType]
  def setPluginType(ntsk: NodeTask, pluginType: PluginType): PluginType = pluginTypes.set(ntsk, pluginType)

  def getPluginType(ntsk: NodeTask): PluginType = {
    val pluginType = pluginTypes.get(ntsk)
    if (pluginType ne null) pluginType
    else PluginType.None
  }

  final case class NodeStats(n: Int, resizes: Int, maxSize: Int)

  // Record all nodes adapted by a plugin, so we can sample them periodically.
  private final class AdaptedNodesOfOnePlugin {
    private var nodes: Array[WeakReference[NodeTask]] = new Array(minNodeArraySize)
    private var nNodes = 0
    private var nextForcedCompact = forceCompactFloor
    private var resizes = 0
    private var maxSize = minNodeArraySize

    private def track(ref: WeakReference[NodeTask]): Unit = this.synchronized {
      if (nNodes > nextForcedCompact)
        removeCollectedOrCompleted()
      if (nNodes == nodes.length) {
        nodes = util.Arrays.copyOf(nodes, nodes.length * 2)
        if (nodes.size > maxSize)
          maxSize = nodes.size
        resizes += 1
      }
      nodes(nNodes) = ref
      nNodes += 1
    }
    def track(ntsk: NodeTask): Unit = track(new WeakReference[NodeTask](ntsk))

    // Called by AdaptedNodesOfAllPlugins.accumulate under forAllRemovables lock
    def accumulateAdaptedNodesFromOnePlugin(other: GuaranteedSnapped[AdaptedNodesOfOnePlugin]): Unit = {
      val snapped = other.getSafe
      val nNodesOther = snapped.nNodes
      val nNodesNew = nNodes + nNodesOther
      if (nNodesNew > nodes.length)
        nodes = util.Arrays.copyOf(nodes, nNodesNew * 2)
      var i = 0
      val os = snapped.nodes
      while (i < nNodesOther) {
        nodes(nNodes + i) = os(i)
        i += 1
      }
      nNodes = nNodesNew
      maxSize = Math.max(maxSize, snapped.maxSize)
      resizes = resizes + snapped.resizes
    }

    // Remove any nodes that are complete or collected.
    private def removeCollectedOrCompleted(): Int = {
      var i = 0
      while (nNodes > i) {
        val ntsk = nodes(i).get()
        if (Objects.isNull(ntsk) || ntsk.isDone) {
          nNodes -= 1
          // if nNodes==i, this is a no-op, and we're about to exit the loop
          nodes(i) = nodes(nNodes)
          nodes(nNodes) = null
        } else {
          i += 1
        }
      }
      // Ideal size is next power of 2
      val newSize = Math.max(minNodeArraySize, 2 * Integer.highestOneBit(nNodes + 1))
      // Resize if this shrinks us by more than a factor of two.
      if (newSize < nodes.size / 2) {
        nodes = util.Arrays.copyOf(nodes, newSize)
        resizes += 1
      }
      // Force a compaction after another nNodes are accumulated
      nextForcedCompact = Math.max(forceCompactFloor, 2 * nNodes)
      i
    }

    def snap(): GuaranteedSnapped[AdaptedNodesOfOnePlugin] = this.synchronized {
      val nt = new AdaptedNodesOfOnePlugin
      // Don't compact() here, as this occurs under forAllRemovables lock
      nt.nodes = util.Arrays.copyOf(nodes, nNodes)
      nt.nNodes = nNodes
      nt.resizes = resizes
      nt.maxSize = maxSize
      guaranteeSnapped(nt)
    }

    // Pass in a Random that's local to whatever thread we're in
    private[graph] def randomUncompletedNode(random: Random): Option[(NodeTask, Int)] = {
      removeCollectedOrCompleted()
      if (nNodes == 0) Option.empty
      else {
        val i = random.nextInt(nNodes)
        for {
          wr <- Option(nodes(i))
          ntsk <- Option(wr.get())
          if !ntsk.isDone
        } yield (ntsk, nNodes)
      }
    }

    private[graph] def randomNode(random: Random, predicate: NodeTask => Boolean): (NodeTask, Int) = {
      if (nNodes == 0) return null
      // Implementation choice is either to copy elements matching predicate, or to evaluate predicate twice per node.
      var i = 0
      var n = 0
      var ntsk: NodeTask = null;
      // Count live nodes matching predicate
      while (i < nNodes) {
        val wr = nodes(i)
        if (Objects.nonNull(wr)) {
          ntsk = wr.get()
          if (Objects.nonNull(ntsk) && predicate(ntsk))
            n += 1
        }
        i += 1
      }
      if (n == 0) return null
      val nFound = n
      n = random.nextInt(nFound)
      i = 0
      while (i < nNodes && n >= 0) {
        val wr = nodes(i)
        if (Objects.nonNull(wr)) {
          ntsk = wr.get()
          if (Objects.nonNull(ntsk) && predicate(ntsk)) n -= 1
          else ntsk = null
        }
        i += 1
      }
      (ntsk, nFound)
    }

    // Methods used by tests
    def countNodes(): Int = {
      removeCollectedOrCompleted()
    }

    def nodeSet(): Set[Int] =
      nodes.map { wr => Option(wr).flatMap(w => Option(w.get())).fold(0)(_.getId) }.toSet - 0

    def stats(): NodeStats = this.synchronized(NodeStats(nNodes, resizes, maxSize))

    def resetStats(): Unit = this.synchronized {
      resizes = 0
      maxSize = 0
    }
  }

  final class AdaptedNodesOfAllPlugins {
    // indexed by plugin id
    private var trackers: Array[AdaptedNodesOfOnePlugin] = new Array(PluginType.maxId * 2)
    private val random = new Random()

    private def ensureAlloc(id: Int): Unit = {
      if (trackers.length <= id) {
        val newLength = Math.max(PluginType.maxId, id) * 2
        trackers = util.Arrays.copyOf(trackers, newLength)
      }
    }

    private def getTracker(id: Int): AdaptedNodesOfOnePlugin = {
      ensureAlloc(id)
      if (Objects.isNull(trackers(id)))
        trackers(id) = new AdaptedNodesOfOnePlugin
      trackers(id)
    }

    def track(pt: PluginType, ntsk: NodeTask): Unit = this.synchronized {
      getTracker(pt.id).track(ntsk)
    }

    private def snapArray(): GuaranteedSnapped[Array[GuaranteedSnapped[AdaptedNodesOfOnePlugin]]] = {
      val opans: Array[GuaranteedSnapped[AdaptedNodesOfOnePlugin]] =
        trackers.map(t => if (t ne null) t.snap() else null.asInstanceOf[GuaranteedSnapped[AdaptedNodesOfOnePlugin]])
      guaranteeSnapped(opans)
    }

    // Called under forAllRemovables lock to pull in adapted nodes across all threads
    def accumulate(nts: AdaptedNodesOfAllPlugins): Unit = if (nts.trackers.length > 0) {
      ensureAlloc(nts.trackers.length - 1)
      val c: Array[GuaranteedSnapped[AdaptedNodesOfOnePlugin]] = nts.snapArray().getSafe
      c.indices.foreach { id =>
        val us = trackers(id)
        val them = c(id)
        if (Objects.nonNull(us) && nonNullSnapped(them))
          us.accumulateAdaptedNodesFromOnePlugin(them)
        else if (Objects.isNull(us))
          trackers(id) = them.getSafe
      }
    }

    private def makeMap[T](f: AdaptedNodesOfOnePlugin => Option[T]): Map[PluginType, T] = {
      val ret = mutable.HashMap.empty[PluginType, T]
      trackers.indices.foreach { id =>
        val t = trackers(id)
        if (Objects.nonNull(t)) {
          f(t).foreach {
            ret += vs(id) -> _
          }
        }
      }
      ret.toMap

    }

    // One randomly chosen node for each plugin.  This will typically be called after accumulating NodeTrackers
    // from every thread.
    def randomUncompletedNodes(): Map[PluginType, (NodeTask, Int)] =
      makeMap(t => t.randomUncompletedNode(random))
    def randomNodes(pred: NodeTask => Boolean): Map[PluginType, (NodeTask, Int)] = {
      makeMap(t => Option(t.randomNode(random, pred)))
    }
    def countNodes(): Map[PluginType, Int] = makeMap(t => Some(t.countNodes()))
    def stats(): Map[PluginType, NodeStats] = makeMap(t => Some(t.stats()))
    def resetStats(): Unit = trackers.foreach(t => if (Objects.nonNull(t)) t.resetStats())

    def nodeSet(): Map[PluginType, Set[Int]] = makeMap(t => Some(t.nodeSet()))
  }

  class PluginTracker {
    val starts = new Counter
    val completed = new Counter
    val fired = new Counter
    val fullWaitTimes = new Counter
    private val counters = Array(starts, completed, fired, fullWaitTimes)
    private val nCounters = counters.size
    val adaptedNodes = new AdaptedNodesOfAllPlugins

    def inFlight: Counter = starts.diff(completed)
    def unFired: Counter = starts.diff(fired)

    def cumulativeCounts: Map[String, Map[String, Long]] =
      Map("starts" -> starts.toMap, "completed" -> completed.toMap, "fired" -> fired.toMap)
    def snapCounts: Map[String, Map[String, Long]] = Map("inFlight" -> inFlight.toMap, "unFired" -> unFired.toMap)

    def diff(rhs: PluginTracker): PluginTracker = {
      val pt = new PluginTracker
      pt.accumulate(this, 1)
      pt.accumulate(rhs, -1)
      pt
    }

    override def toString: String =
      s"$cumulativeCounts $snapCounts fullWaitTimes=$fullWaitTimes adaptedNodes=${adaptedNodes.stats()}"
    def accumulate(from: PluginTracker, mult: Long): Unit = {
      var i = 0
      while (i < nCounters) {
        counters(i).accumulate(from.counters(i), mult)
        i += 1
      }
      adaptedNodes.accumulate(from.adaptedNodes)
    }
  }

  def snapAggregatePluginCounts(): PluginTracker = PluginType.synchronized {
    val pc = new PluginTracker
    OGLocalTables.forAllRemovables((rt: RemovableLocalTables) => pc.accumulate(rt.pluginTracker, 1))
    pc
  }

  def fire(ns: Iterable[NodeTask]): Unit = fire(ns, PluginType.Other)
  def fire(ns: Iterable[NodeTask], default: PluginType): Unit = {
    OGLocalTables.borrow { lt =>
      ns.foreach { n =>
        val pt0 = n.getReportingPluginType
        if (Objects.isNull(pt0)) {
          MonitoringBreadcrumbs.sendGraphFatalErrorCrumb(
            "NodeTask with no plugin type",
            ntsk = n,
            exception = new NullPointerException(),
            logFile = null,
            logMsg = null)
        }
        val pt = if (Objects.isNull(pt0)) default else pt0
        n.markPluginFired()
        lt.pluginTracker.fired.update(pt, 1)
      }
    }
  }
}

/**
 * Base class for Scheduler plugins
 */
abstract class SchedulerPlugin {

  /**
   * Returns true if NodeTask has been "taken" and will be handled by plugin false otherwise (causes the regular local queue schedule.
   * SHOULD NOT BE CALLED DIRECTLY; use readapt instead.
   */
  protected[this] def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean

  /**
   * Called by adapt method of the `from` plugin to assign the node to us instead.
   */
  final def readapt(n: NodeTask, ec: OGSchedulerContext): Boolean = adaptInternal(n, ec)

  private[graph] final def adaptInternal(n: NodeTask, ec: OGSchedulerContext): Boolean = {
    val old = n.setPluginType(pluginType)
    val ret = adapt(n, ec)
    if (!ret) {
      n.setPluginType(old)
    }
    ret
  }

  /**
   * Ideally, this will be overridden with something more deliberately chosen
   */
  val pluginType: PluginType = {
    var clz: Class[_] = this.getClass
    while (clz.getSimpleName.isEmpty) { clz = clz.getSuperclass }
    PluginType(clz.getSimpleName)
  }

  /**
   * returns user-readable information to explain the likely cause of the graph stalling on this plugin it's enough to
   * just override pluginType optional stallReason provides a nicer text message if pluginType is too cryptic optional
   * stallRequests provides further information about what the plugin is busy with
   */
  def stallReason: String =
    if (pluginType == PluginType.Other) PluginType.unknownReasonStallMsg else s"waiting for ${pluginType.name}"

  final def graphStallInfo(nodeTaskInfo: NodeTaskInfo): GraphStallInfo =
    GraphStallInfo(pluginType, stallReason, stallRequests(nodeTaskInfo))

  final def graphStallInfoForTask(nodeTaskInfo: NodeTaskInfo, nt: NodeTask): Option[GraphStallInfo] = {
    // to avoid contradicting stall messages (unknown from plugin, but defined in NodeTask extra info)
    if (stallReason.contains(PluginType.unknownReasonStallMsg) && StallInfoAppender.getExtraData(nt) != null)
      None
    else
      Some(GraphStallInfo(pluginType, stallReason, stallRequests(nodeTaskInfo)))
  }

  // get a full list of outstanding batches / requests in flight
  def stallRequests(nodeTaskInfo: NodeTaskInfo): Option[RequestsStallInfo] = None
}

final case class GraphStallInfo(
    pluginType: PluginType,
    message: String,
    requestsStallInfo: Option[RequestsStallInfo] = None)

object GraphStallInfo {
  def apply(pluginOpt: Option[SchedulerPlugin], nodeTaskInfo: NodeTaskInfo): GraphStallInfo = pluginOpt match {
    case Some(plugin) => plugin.graphStallInfo(nodeTaskInfo)
    case None => apply(PluginType.Other, nodeTaskInfo.name)
  }
}