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
package optimus.graph.diagnostics.gridprofiler

import optimus.graph.diagnostics.gridprofiler.GridProfiler.defaultCombOp
import optimus.graph.diagnostics.gridprofiler.GridProfiler.nextMetricName

import scala.collection.mutable
import scala.collection.{concurrent => c}
import scala.collection.{mutable => m}
import java.util.ArrayList
import optimus.scalacompat.collection._
import scala.jdk.CollectionConverters._

object GridProfilerData {
  // This is the static storage of all gridprofiler data
  // All APIs in this class should be only used by optimus.graph.diagnostics classes and by the tests
  //
  // There are three independent keys:
  // 1. metric name: dal stats, hotspots, etc. This can be a user-defined String (via recordCustomCounter)
  // 2. block scope: Represents location in code, e.g. user-defined grouping such as trader book
  // 3. aggregation key, default dist task id: each task id is also associated with dist engine and dist depth, and we offer aggregation by engine, by depth, client/remote, and full
  //
  // block scope is naturally Seq[ProfilerScope], but within each JVM, it is Int: see GridProfiler.blockIDRegistry. The ints are sequential starting at zero, and can be be used as array index
  // metric name is naturally either GridProfiler.Metric or String, but within each JVM it is an Int: see GridProfiler.metricNameRegistry. The ints are sequential starting at zero, and can be be used as array index
  // dist task id is replaced by aggregation key when aggregation is performed (when sending from engine to client and on user API boundaries), but it's still a String

  // This could be done as Guava's "new HashBasedTable[Int, String, MetricData]", but it's just Map<R, Map<C, V>> under the hood anyway
  // Putting Scope first because profiler.result will index on that, while there are no queries that index on task/aggregation key without also indexing on scopes
  // the index in the ArrayBuffer is the metric name; some values may be null
  private[optimus] val data = c.TrieMap.empty[Int /*Scope*/, c.Map[String /*Task*/, MetricData]]

  // The type of Value is not actually "Any": it must be Serializable and supported by GridProfiler.defaultCombOp
  type MetricData = mutable.Buffer[Any /*Value*/ ]
  def MetricData(initCapacity: Int): MetricData = {
    // TODO (OPTIMUS-70380): Scala 2.13's ArrayBuffer, which used to be used here,
    //  now throws a ConcurrentModificationException
    // when an element is written by another thread during iteration. This appears to be an overreach in Scala 2.13's
    // implementation of fail-fast iteration -- Java only fails when the operation alters the structure of underlying
    // data structure, e.g. an append that requires a capacity increase of an ArrayList.
    //
    // To get things working in 2.13, we use a Java ArrayList wrapped as a s.c.mutable.Buffer. This is API
    // compatible with existing clients of this code.
    //
    // It is hard to reason about the thread safety here. A cleaner implementation IMO would avoid resizing the
    // Seq mid-flight, perhaps by reserving it for the standard metrics and using a Map for custom metries (if any):
    //
    // class MetricData {
    //    // where numStandardMetrics is a final val computed after registration of
    //    private val standardData = Array[Any](GridProfile.numStandardMetrics)
    //    private val customData = c.TrieMap[CustomMetric, Any]()
    //    ...
    // }
    //
    // A variation of this would be:
    //     private val standardData = new j.u.c.AtomicReferenceArray[Any](GridProfile.numStandardMetrics)
    //     private val customData = j.u.c.ConcurrentHashMap[CustomMetric, Any]()
    //
    // To allow for use of the AtomicReferenceArray#accumulateAndGet / ConcurrentHashMap.compute for lock-free
    // methods to avoid TOUTOA bugs that seem possible with the current implementation.
    new ArrayList[Any](initCapacity).asScala
  }

  // backup for when local distribution is in effect
  private[optimus] val suspendedData = m.Map.empty[Int, Map[Int, Map[String, MetricData]]]

  // property name -> tweak ID, used for remapping dependency data between JVMs (lives here because this is where all
  // other data that is reported from engines is stored).
  // Note: start with INTERNAL_TweakMarker -> 1 so we don't ever remap anything back to 1!
  private[gridprofiler] val nameToTweakID =
    mutable.Map[String, Int]("INTERNAL_TweakMarker" -> 1) // [SEE_REMAP_TWEAK_DEPS]

  //
  // Insertion API
  //
  private[optimus] def put(scope: Int, task: String, key: Int, value: Any): Unit = {

    val row: c.Map[String, MetricData] = data.getOrElse(
      scope, {
        val emptyRow = c.TrieMap.empty[String, MetricData]
        data.putIfAbsent(scope, emptyRow).getOrElse(emptyRow)
      })

    val metrics: MetricData = row.getOrElse(
      task, {
        val emptyMetrics = MetricData(nextMetricName.get)
        row.putIfAbsent(task, emptyMetrics).getOrElse(emptyMetrics)
      })

    metrics.synchronized {
      while (metrics.size <= key) metrics.append(null)
      val old = metrics(key)
      metrics(key) = if (old == null) value else defaultCombOp(old, value)
    }
  }

  //
  // Extraction APIs
  //

  // select where
  //   scope == scope
  // and
  //   key == key
  // returns Map[Task, T]
  private[optimus] def get[T](scope: Int, key: Int): Map[String, T] = {
    val res = m.Map.empty[String, T]

    for (
      row <- data.get(scope);
      (task, metrics) <- row
    ) {
      metrics.synchronized {
        if (key < metrics.size && metrics(key) != null) {
          res.put(task, metrics(key).asInstanceOf[T])
        }
      }
    }

    res.toMap
  }

  // select where
  //   scope in scopes
  // and
  //   key == key
  // returns Map[Scope, Map[Task, T]
  private[optimus] def get[T](scopes: collection.Seq[Int], key: Int): Map[Int, Map[String, T]] = {
    val res = m.Map.empty[Int, m.Map[String, T]]

    def emptyRow = m.Map.empty[String, T]

    for (
      (scope, row) <- data.filterKeysNow(scopes.toSet);
      (task, metrics) <- row
    ) {
      metrics.synchronized {
        if (key < metrics.size && metrics(key) != null) {
          val row = res.getOrElseUpdate(scope, emptyRow)
          row.put(task, metrics(key).asInstanceOf[T])
        }
      }
    }

    res.mapValuesNow(_.toMap).toMap
  }

  // select where
  //   key == key
  // returns Map[Scope, Map[Task, T]
  private[optimus] def getAll[T](key: Int): Map[Int, Map[String, T]] = {
    val res = m.Map.empty[Int, m.Map[String, T]]

    def emptyRow = m.Map.empty[String, T]

    for (
      (scope, row) <- data;
      (task, metrics) <- row
    ) {
      metrics.synchronized {
        if (key < metrics.size && metrics(key) != null) {
          val row = res.getOrElseUpdate(scope, emptyRow)
          row.put(task, metrics(key).asInstanceOf[T])
        }
      }
    }

    res.mapValuesNow(_.toMap).toMap
  }

  // select where
  //  scope in scopes
  // and
  //  key is custom
  // returns Map[Scope, Map[Task, Map[Key(as String), Any]]]
  private[optimus] def getCustom(scopes: collection.Seq[Int]): Map[Int, Map[String, Map[String, Any]]] = {
    val res = m.Map.empty[Int, m.Map[String, m.Map[String, Any]]]
    def emptyRow = m.Map.empty[String, m.Map[String, Any]]
    def emptyMetrics = m.Map.empty[String, Any]

    for (
      (scope, row) <- data.filterKeysNow(scopes.toSet);
      (task, metrics) <- row
    ) {
      metrics.synchronized {
        var idx = GridProfiler.Metric.maxId // first custom key
        while (idx < metrics.size) {
          if (metrics(idx) != null) {
            val row = res.getOrElseUpdate(scope, emptyRow)
            val data = row.getOrElseUpdate(task, emptyMetrics)
            data.put(GridProfiler.metricIDToName(idx).get, metrics(idx))
          }
          idx += 1
        }
      }
    }

    res.mapValuesNow(x => x.mapValuesNow(_.toMap).toMap).toMap
  }

  // select where
  //  key is custom
  // returns Map[Scope, Map[Task, Map[Key(as String), Any]]]
  private[optimus] def getAllCustom: Map[Int, Map[String, Map[String, Any]]] = {
    val res = m.Map.empty[Int, m.Map[String, m.Map[String, Any]]]
    def emptyRow = m.Map.empty[String, m.Map[String, Any]]
    def emptyMetrics = m.Map.empty[String, Any]

    for (
      (scope, row) <- data;
      (task, metrics) <- row
    ) {
      metrics.synchronized {
        var idx = GridProfiler.Metric.maxId // first custom key
        while (idx < metrics.size) {
          if (metrics(idx) != null) {
            val row = res.getOrElseUpdate(scope, emptyRow)
            val data = row.getOrElseUpdate(task, emptyMetrics)
            data.put(GridProfiler.metricIDToName(idx).get, metrics(idx))
          }
          idx += 1
        }
      }
    }

    res.mapValuesNow(x => x.mapValuesNow(_.toMap).toMap).toMap
  }

  // select where
  //  key == key
  // returns Map[Task -> Value]
  private[optimus] def getCombined[T](key: Int): Map[String, T] = {
    val res = m.Map.empty[String, T]

    for (
      (scope, row) <- data;
      (task, metrics) <- row
    ) {
      metrics.synchronized {
        if (key < metrics.size && metrics(key) != null) {
          val v = metrics(key).asInstanceOf[T]
          res.get(task) match {
            case Some(old) => res(task) = defaultCombOp(old, v).asInstanceOf[T]
            case None      => res(task) = v
          }
        }
      }
    }

    res.toMap
  }

  // select where
  //  key >= Metrics.maxId
  // returns Map[Task -> Key (converted to String) -> Value]
  private[optimus] def getCombinedCustom: Map[String, Map[String, Any]] = {
    val res = m.Map.empty[String, m.Map[String, Any]]
    def emptyMetrics = m.Map.empty[String, Any]

    for (
      (scope, row) <- data;
      (task, metrics) <- row
    ) {
      metrics.synchronized {
        var idx = GridProfiler.Metric.maxId // first custom key
        while (idx < metrics.size) {
          if (metrics(idx) != null) {
            val skey = GridProfiler.metricIDToName(idx).get
            val data = res.getOrElseUpdate(task, emptyMetrics)
            val v = metrics(idx)
            data.get(skey) match {
              case Some(old) => data(skey) = defaultCombOp(old, v)
              case None      => data(skey) = v
            }
          }
          idx += 1
        }
      }
    }

    res.mapValuesNow(_.toMap).toMap
  }

  //
  // Removal APIs
  //

  // drop everything
  private[optimus] def clear(): Unit = {
    data.clear()
  }

  // extract and delete all metrics for the given scope
  private[optimus] def removeByScope(scope: Int): Map[String, MetricData] = {
    val empty = m.Map.empty[String, MetricData]
    val removed = data.remove(scope)
    removed.getOrElse(empty).toMap
  }

  // extract and delete all metrics for the given taskId
  private[optimus] def removeByTask(task: String): Map[Int, MetricData] = {
    val r = m.Map.empty[Int, MetricData]
    for ((scope, row) <- data) {
      row.remove(task).map(r.put(scope, _))
    }
    r.toMap
  }

  // extract and delete all metrics
  // this is called on the engine from the constructor of ProfileData
  private[optimus] def removeAll(): Map[Int, Map[String, MetricData]] = {
    val r = m.Map.empty[Int, m.Map[String, MetricData]]
    for (scope <- data.keySet) {
      data.remove(scope).map(r.put(scope, _))
    }
    r.mapValuesNow(_.toMap).toMap
  }

  // extracts and deletes custom metrics for the given key./scope
  // this is called through the user-facing API GridProfiler.removeCustomMetrics
  private[optimus] def removeByKey(key: Int, sc: Int): collection.Seq[Any] = {
    val res = MetricData(0)
    for (
      map <- data.get(sc);
      (task, metric) <- map
    ) {
      metric.synchronized {
        if (key < metric.size) {
          val old = metric(key)
          metric(key) = null
          res += old
          if (metric.forall(_ == null))
            map.remove(task, metric)
        }
      }
    }
    res
  }

  //
  // Aggregation APIs
  //

  private[optimus] def mergeMetricValues(a1: MetricData, a2: MetricData): MetricData = {
    val sz = math.max(a1.size, a2.size)
    val res = MetricData(sz)
    var i = 0
    while (i < sz) {
      val v1 = if (i < a1.size) a1(i) else null
      val v2 = if (i < a2.size) a2(i) else null
      res.append((v1, v2) match {
        case (null, null) => null
        case (null, y)    => y
        case (x, null)    => x
        case (x, y)       => defaultCombOp(x, y)
      })
      i += 1
    }
    res
  }

  private[optimus] def aggregationKey(k: String, agg: AggregationType.Value) = agg match {
    case AggregationType.NONE =>
      k
    case AggregationType.ENGINE =>
      GridProfiler.getTaskRegistry.get(k).map(_._1).getOrElse(k)
    case AggregationType.DEPTH =>
      GridProfiler.getTaskRegistry.get(k).map(m => GridProfiler.depthKey(m._2.size + 1)).getOrElse(k)
    case AggregationType.CLIENTANDREMOTE =>
      if (k == GridProfiler.clientKey) k
      else GridProfiler.remoteKey
    case AggregationType.AGGREGATED =>
      GridProfiler.aggregatedKey
  }

  private def aggregateAndUpdate[T](result: m.Map[String, T], nk: String, v: T, f: (T, T) => T): Unit = {
    result.put(nk, result.get(nk).map(f(_, v)).getOrElse(v))
  }

  // apply AggregationType (from --profile-aggregation) to all metrics at once (called when preparing ProfilerData on the grid)
  private[optimus] def aggregate(data: Map[String, MetricData], agg: AggregationType.Value): Map[String, MetricData] = {

    val result = m.Map.empty[String, MetricData]
    for ((k, v) <- data) {
      val nk = aggregationKey(k, agg)
      aggregateAndUpdate(result, nk, v, mergeMetricValues)
    }
    result.toMap
  }

  // apply AggregationType (from --profile-aggregation) to metrics of the specified type (called on user API boundaries)
  // may include an extra "local" value for metrics whose local value comes from live storage (Hotspots and Scheduler))
  private[optimus] def aggregateSingle[T](
      data: Map[String, T],
      local: Option[T],
      agg: AggregationType.Value): Map[String, T] = {

    val result = m.Map.empty[String, T]

    for ((k, v) <- data) {
      val nk = aggregationKey(k, agg)
      aggregateAndUpdate(result, nk, v, (x: T, y: T) => defaultCombOp(x, y).asInstanceOf[T])
    }
    for (v <- local) {
      val nk = aggregationKey(GridProfiler.clientKey, agg)
      aggregateAndUpdate(result, nk, v, (x: T, y: T) => defaultCombOp(x, y).asInstanceOf[T])
    }

    result.toMap
  }

  // Local recursion support
  private[optimus] def push(depth: Int): Unit = {
    val old = suspendedData.put(depth, removeAll())
    if (old.nonEmpty) GridProfiler.log.error(s"Profiler metrics were suspended twice at local recursion depth $depth")
  }

  private[optimus] def pop(depth: Int): Unit = {
    val old = suspendedData.remove(depth)
    if (old.isEmpty) {
      GridProfiler.log.error(s"Suspended profiler metrics not found at local recursion depth $depth")
    } else {
      for (
        (scope, row) <- old.get;
        (task, metrics) <- row
      ) {
        var idx = 0
        while (idx < metrics.size) {
          if (metrics(idx) != null)
            put(scope, task, idx, metrics(idx))
          idx += 1
        }
      }
    }
  }
}
