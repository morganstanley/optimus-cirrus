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

import com.google.common.util.concurrent.AtomicDouble
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.ElemOrElems
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.platform.util.Log
import optimus.scalacompat.collection._
import spray.json.DefaultJsonProtocol._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * The ObservationRegistry establishes a simple pattern to collect counter-based metrics.
 * There are groups of Counters which can be snapshot together
 * These snapshots can be diffed together (and derived metrics generated)
 * Then the diffs can be published into breadcrumbs (with specific names)
 *
 * So there are Counters, Snapshots, Diffs
 *
 * These classes aim to establish an API of maximal simplicity, where places that increment counters
 * are trivial to find. Counters need to be fast, and thread-safe (for update and read).
 * At some scale, contention may become a problem, but most usages don't hit this limitation.
 *
 * These counters are JVM-wide
 * They can be sampled at any point
 *
 * Key principle is that simplest case is simple
 * Explicit counters, generic snapshots, generic diffs, generic json (breadcrumb)
 * In cases where you want to do more complex things at snapshot, diff and json time, you can create custom classes
 * to contain logic, and store values in named variables, rather than as values in Maps
 */

object ObservationRegistry {
  val instance = new ObservationRegistry()
}

class ObservationRegistry extends Log {
  private val counterGroups = new ConcurrentHashMap[String, CounterGroup]()

  /**
   * explicit hook point for registering counter groups
   * simple to find all call points to see full set of counter groups
   *
   * additional CounterGroups can be registered at any point
   * They are usually registered as part of class loading on the specific *Counters object
   */
  def registerCounterGroup(group: CounterGroup): Unit = {
    counterGroups.put(group.groupName, group)
  }

  def snapshotAll(timestampMs: Long = System.currentTimeMillis()): FullObservationSnapshot = {
    import scala.jdk.CollectionConverters._

    val map = counterGroups
      .values()
      .asScala
      .map { group =>
        group.groupName -> group.snapshot()
      }
      .toMap
    FullObservationSnapshot(timestampMs, map)
  }

  def diffBetween(snapshot1: FullObservationSnapshot, snapshot2: FullObservationSnapshot): FullObservationDiff = {
    // use the keys from snapshot2 (the latest snapshot)
    // it may contain addition groupKeys not present in snapshot1
    val diffs = snapshot2.groups.map { case (groupName, group2) =>
      val group1 = snapshot1.getOrMakeGroup(groupName)
      groupName -> group2.diffSince(group1)
    }
    FullObservationDiff(
      snapshot1.timestampMs,
      snapshot2.timestampMs,
      snapshot2.timestampMs - snapshot1.timestampMs,
      diffs)
  }
}

final case class FullObservationSnapshot(timestampMs: Long, groups: Map[String, SnapshotGroup]) {
  def getOrMakeGroup(groupName: String) = groups.get(groupName).getOrElse(SnapshotGroupEmpty(groupName))
}

/**
 * Diff between two snapshots
 */
final case class FullObservationDiff(
    timestampMs1: Long,
    timestampMs2: Long,
    durationMs: Long,
    groups: Map[String, DiffGroup]) {
  def combine(that: FullObservationDiff): FullObservationDiff = {
    val groupNames = this.groups.keySet ++ that.groups.keySet
    val combinedGroups = groupNames.map { groupName =>
      val g1 = this.groups.get(groupName)
      val g2 = that.groups.get(groupName)
      (g1, g2) match {
        case (Some(group1), Some(group2)) =>
          groupName -> group1.combine(group2)
        case (Some(group1), None) =>
          groupName -> group1
        case (None, Some(group2)) =>
          groupName -> group2
        case (None, None) =>
          throw new IllegalStateException(s"should never happen, group $groupName not found in either diff")
      }
    }.toMap
    val ts1 = (this.timestampMs1, that.timestampMs1) match {
      case (0, t) => t
      case (t, 0) => t
      case (a, b) => Math.min(a, b)
    }
    val ts2 = (this.timestampMs2, that.timestampMs2) match {
      case (0, t) => t
      case (t, 0) => t
      case (a, b) => Math.max(a, b)
    }
    FullObservationDiff(
      ts1,
      ts2,
      this.durationMs + that.durationMs,
      combinedGroups
    )
  }

  def asMapStringMapWithNoZeroes(): Map[String, Map[String, String]] = {
    groups.flatMap { case (groupName, diffGroup) =>
      val map = diffGroup.asStringMapWithNoZeroes()
      if (map.isEmpty) None else Some(groupName -> map)
    }
  }

  def asBreadcrumbElem: Elems = {
    val map = asMapStringMapWithNoZeroes()
    if (map.isEmpty) Elems.Nil
    else {
      val prop = Properties.adhocProperty[Map[String, Map[String, String]]]("observationRegistry")
      Elems(prop -> map)
    }
  }
}

object FullObservationDiff {
  val Empty = FullObservationDiff(0, 0, 0, Map())
}

trait Counter {
  val name: String
}

class LongCounter(val name: String) extends Counter {
  private val counter: AtomicLong = new AtomicLong()

  def increment(): Unit = counter.incrementAndGet()
  def add(value: Long): Unit = counter.addAndGet(value)
  def get(): Long = counter.get()
}

class DoubleCounter(val name: String) extends Counter {
  private val counter: AtomicDouble = new AtomicDouble()

  def increment(): Unit = add(1)
  def add(value: Double): Unit = counter.addAndGet(value)
  def get(): Double = counter.get()
}

trait CounterGroup {
  def groupName: String
  def snapshot(): SnapshotGroup
}

class CounterGroupSimple(
    val groupName: String,
    val longCounters: Seq[LongCounter],
    val doubleCounters: Seq[DoubleCounter])
    extends CounterGroup {
  def snapshot(): SnapshotGroup = {
    SnapshotGroupSimple(
      groupName,
      longCounters.map { counter =>
        counter.name -> counter.get()
      }.toMap,
      doubleCounters.map { counter =>
        counter.name -> counter.get()
      }.toMap
    )
  }
}

trait SnapshotGroup {
  def groupName: String
  def getLong(counterName: String): Long
  def getDouble(counterName: String): Double

  /**
   * always diff the latest snapshot from the previous snapshot as additional groups/counters may be captured
   */
  def diffSince(that: SnapshotGroup): DiffGroup
}

final case class SnapshotGroupEmpty(groupName: String) extends SnapshotGroup {
  def getLong(counterName: String): Long = 0
  def getDouble(counterName: String): Double = 0
  // intentionally not supported, there should never be a SnapshotGroupEmpty as the second snapshot
  def diffSince(that: SnapshotGroup): DiffGroup = {
    throw new RuntimeException("diffSince not supported for SnapshotGroupEmpty")
  }
}

final case class SnapshotGroupSimple(groupName: String, longMap: Map[String, Long], doubleMap: Map[String, Double])
    extends SnapshotGroup {
  def getLong(counterName: String): Long = longMap(counterName)
  def getDouble(counterName: String): Double = doubleMap(counterName)
  def diffSince(that: SnapshotGroup): DiffGroup = {
    DiffGroupSimple(
      this.groupName,
      this.longMap.map { case (key, value) =>
        key -> (value - that.getLong(key))
      },
      this.doubleMap.map { case (key, value) =>
        key -> (value - that.getDouble(key))
      }
    )
  }
}

trait DiffGroup {
  def groupName: String
  def longMap: Map[String, Long]
  def doubleMap: Map[String, Double]
  def combine(that: DiffGroup): DiffGroup

  def asStringMapWithNoZeroes(): Map[String, String] =
    doubleMap.filter { case (_, v) => v != 0d }.mapValuesNow(_.toString) ++
      longMap.filter { case (_, v) => v != 0L }.mapValuesNow(_.toString)

  def asBreadcrumbElem: ElemOrElems = {
    val map = asStringMapWithNoZeroes()
    if (map.isEmpty) Elems.Nil
    else {
      val prop = Properties.adhocProperty[Map[String, String]](groupName)
      prop -> map
    }
  }
}

final case class DiffGroupSimple(groupName: String, longMap: Map[String, Long], doubleMap: Map[String, Double])
    extends DiffGroup {
  def combine(that: DiffGroup): DiffGroup = {
    val longKeys = this.longMap.keySet ++ that.longMap.keySet
    val combinedLongKeys = longKeys.map { key =>
      val v1 = this.longMap.getOrElse(key, 0L)
      val v2 = that.longMap.getOrElse(key, 0L)
      key -> (v1 + v2)
    }.toMap
    val doubleKeys = this.doubleMap.keySet ++ that.doubleMap.keySet
    val combinedDoubleKeys = doubleKeys.map { key =>
      val v1 = this.doubleMap.getOrElse(key, 0d)
      val v2 = that.doubleMap.getOrElse(key, 0d)
      key -> (v1 + v2)
    }.toMap
    DiffGroupSimple(
      this.groupName,
      combinedLongKeys,
      combinedDoubleKeys
    )
  }

}
