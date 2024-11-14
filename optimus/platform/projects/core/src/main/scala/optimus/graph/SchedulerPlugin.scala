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
import optimus.graph.diagnostics.sampling.SamplingProfiler.NANOSPERMILLI
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
      if (!ntsk.getAndSetPluginTracked()) {
        // starts only increases, so you can calculate the number of starts in a time range
        tracker.starts.update(this, 1)
        if (!tracker.adaptedNodes.track(this, ntsk))
          tracker.overflow.update(this, 1)
      }
    }
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

    def diff(pc: Counter*): Counter = {
      val c = snap().getSafe
      pc.foreach(c.accumulate(_, -1))
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
    def toMapMillis: Map[String, Long] = this.synchronized {
      val vv = for (i <- counts.indices if counts(i) != 0) yield vs(i).name -> counts(i) / NANOSPERMILLI
      vv.toMap
    }

    override def toString: String = toMap.toString
  }

  private val maxNodeArraySize = DiagnosticSettings.maxPluginNodesTracked
  private val forceCompactFloor = 100
  private val minNodeArraySize = 4

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

    // If accumulating nodes globally and under lock, use persistent snap destination to avoid repeatedly allocating the
    // node array.
    private var snapBuffer: AdaptedNodesOfOnePlugin = null

    def resetNodesButRetainArray(): Unit = this.synchronized {
      nNodes = 0
    }

    private def track(ref: WeakReference[NodeTask]): Boolean = this.synchronized {
      if (nNodes > nextForcedCompact)
        removeCollectedOrCompleted()
      if (nNodes > maxNodeArraySize)
        false
      else {
        if (nNodes == nodes.length) {
          nodes = util.Arrays.copyOf(nodes, nodes.length * 2)
          if (nodes.size > maxSize)
            maxSize = nodes.size
          resizes += 1
        }
        nodes(nNodes) = ref
        nNodes += 1
      }
      true
    }
    private[graph] def track(ntsk: NodeTask): Boolean = track(new WeakReference[NodeTask](ntsk))

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

    def snap(intoBuffer: Boolean): GuaranteedSnapped[AdaptedNodesOfOnePlugin] = this.synchronized {
      val nt = if (intoBuffer) {
        // If snapping into the persistent global store we only grow the array, never shrink it.
        if (Objects.isNull(snapBuffer))
          snapBuffer = new AdaptedNodesOfOnePlugin
        if (snapBuffer.nodes.length < nodes.length)
          snapBuffer.nodes = new Array(nodes.length * 3 / 2)
        System.arraycopy(nodes, 0, snapBuffer.nodes, 0, nNodes)
        snapBuffer
      } else {
        // Compact if we're not snapping into the global store, in which case compaction will occur later.
        removeCollectedOrCompleted()
        val nt = new AdaptedNodesOfOnePlugin
        nt.nodes = util.Arrays.copyOf(nodes, nodes.length)
        nt
      }
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
    private var individualPlugins: Array[AdaptedNodesOfOnePlugin] = new Array(PluginType.maxId * 2)
    private val random = new Random()

    def resetNodesButRetainArray(): Unit = this.synchronized {
      individualPlugins.foreach(ip => if (Objects.nonNull(ip)) ip.resetNodesButRetainArray())
    }

    private def ensureAlloc(id: Int): Unit = {
      if (individualPlugins.length <= id) {
        val newLength = Math.max(PluginType.maxId, id) * 2
        individualPlugins = util.Arrays.copyOf(individualPlugins, newLength)
      }
    }

    private def getAdaptedNodeTracker(id: Int): AdaptedNodesOfOnePlugin = {
      ensureAlloc(id)
      if (Objects.isNull(individualPlugins(id)))
        individualPlugins(id) = new AdaptedNodesOfOnePlugin
      individualPlugins(id)
    }

    def track(pt: PluginType, ntsk: NodeTask): Boolean = this.synchronized {
      getAdaptedNodeTracker(pt.id).track(ntsk)
    }

    private def snapArray(intoBuffer: Boolean): GuaranteedSnapped[Array[GuaranteedSnapped[AdaptedNodesOfOnePlugin]]] = {
      val opans: Array[GuaranteedSnapped[AdaptedNodesOfOnePlugin]] =
        individualPlugins.map(t =>
          if (t ne null) t.snap(intoBuffer) else null.asInstanceOf[GuaranteedSnapped[AdaptedNodesOfOnePlugin]])
      guaranteeSnapped(opans)
    }

    def accumulate(from: AdaptedNodesOfAllPlugins, intoBuffer: Boolean): Unit = if (from.individualPlugins.length > 0) {
      ensureAlloc(from.individualPlugins.length - 1)
      val c: Array[GuaranteedSnapped[AdaptedNodesOfOnePlugin]] = from.snapArray(intoBuffer).getSafe
      c.indices.foreach { id =>
        val us = individualPlugins(id)
        val them = c(id)
        if (Objects.nonNull(us) && nonNullSnapped(them))
          us.accumulateAdaptedNodesFromOnePlugin(them)
        else if (Objects.isNull(us))
          individualPlugins(id) = them.getSafe
      }
    }

    private def makeMap[T](f: AdaptedNodesOfOnePlugin => Option[T]): Map[PluginType, T] = {
      val ret = mutable.HashMap.empty[PluginType, T]
      individualPlugins.indices.foreach { id =>
        val t = individualPlugins(id)
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
    def resetStats(): Unit = individualPlugins.foreach(t => if (Objects.nonNull(t)) t.resetStats())

    def nodeSet(): Map[PluginType, Set[Int]] = makeMap(t => Some(t.nodeSet()))
  }

  class PluginTracker {
    val starts = new Counter
    val completed = new Counter
    val fired = new Counter
    val neverFired = new Counter // completed before being fired - a nonfatal error
    val fullWaitTimes = new Counter
    val overflow = new Counter
    private val counters = Array(starts, completed, fired, neverFired, fullWaitTimes, overflow)
    private val nCounters = counters.size
    val adaptedNodes = new AdaptedNodesOfAllPlugins

    def inFlight: Counter = starts.diff(completed)
    def unFired: Counter = starts.diff(fired, neverFired)

    def cumulativeCounts: Map[String, Map[String, Long]] =
      Map(
        "starts" -> starts.toMap,
        "completed" -> completed.toMap,
        "fired" -> fired.toMap,
        "neverFired" -> neverFired.toMap)
    def snapCounts: Map[String, Map[String, Long]] = Map("inFlight" -> inFlight.toMap, "unFired" -> unFired.toMap)

    def diff(rhs: PluginTracker): PluginTracker = {
      val pt = new PluginTracker
      pt.accumulate(from = this, mult = 1, intoBuffer = false)
      pt.accumulate(from = rhs, mult = -1, intoBuffer = false)
      pt
    }

    override def toString: String =
      s"$cumulativeCounts $snapCounts fullWaitTimes=$fullWaitTimes adaptedNodes=${adaptedNodes.stats()}"
    def accumulate(from: PluginTracker, mult: Long, intoBuffer: Boolean): Unit = {
      var i = 0
      while (i < nCounters) {
        counters(i).accumulate(from.counters(i), mult)
        i += 1
      }
      adaptedNodes.accumulate(from.adaptedNodes, intoBuffer)
    }
  }

  def snapAggregatePluginCountsIntoGlobalBuffer(): PluginTracker = PluginType.synchronized {
    val pc = new PluginTracker
    OGLocalTables.forAllRemovables((rt: RemovableLocalTables) => pc.accumulate(rt.pluginTracker, 1, true))
    pc
  }

  def completed(eq: EvaluationQueue, task: NodeTask): Unit = {
    if (task.pluginTracked) {
      val pt = getPluginType(task)
      if (Objects.nonNull(pt)) {
        val wasFired = task.getAndSetPluginFired()
        OGLocalTables.borrow(
          eq,
          { lt: OGLocalTables =>
            {
              if (!wasFired) {
                lt.pluginTracker.neverFired.update(pt, 1)
              }
              lt.pluginTracker.completed.update(pt, 1)
            }
          })
      }
    }
  }

  def fire(ntsk: NodeTask): Unit = fire(Seq(ntsk))

  def fire(ns: Iterable[NodeTask]): Unit = {
    OGLocalTables.borrow { lt =>
      ns.foreach { n =>
        if (!n.getAndSetPluginFired()) {
          val pt = n.getReportingPluginType
          if (Objects.isNull(pt)) {
            MonitoringBreadcrumbs.sendGraphFatalErrorCrumb(
              "NodeTask with no plugin type",
              ntsk = n,
              exception = new NullPointerException(),
              logFile = null,
              logMsg = null
            )
            return
          }
          lt.pluginTracker.fired.update(pt, 1)
          // If we're fired without being adapted, record the adapting now
          if (!n.pluginTracked) {
            pt.recordLaunch(lt, n)
          }
        }
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
    val old = PluginType.setPluginType(n, pluginType)
    val ret = adapt(n, ec)
    if (!ret) {
      PluginType.setPluginType(n, old)
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
    case None         => apply(PluginType.Other, nodeTaskInfo.name)
  }
}
