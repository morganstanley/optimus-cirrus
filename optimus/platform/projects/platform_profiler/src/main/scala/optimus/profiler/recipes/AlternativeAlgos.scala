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
package optimus.profiler.recipes

import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap
import optimus.graph.OGTrace
import optimus.graph.diagnostics.PNodeTask
import optimus.platform.entersGraph
import optimus.platform.inputs.GraphInputConfiguration
import optimus.profiler.DebuggerUI.foreachAncestor

import java.util.{IdentityHashMap => JIdentityHashMap}
import java.util.{Arrays => JArrays}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AlternativeAlgos {
  // Approximate count of number of distinct items.  See https://en.wikipedia.org/wiki/HyperLogLog
  private class LogLogCounter(k: Int) {
    private val m = 1 << k
    private val H = 32
    private val estimators = new Array[Int](m)
    def add(value: AnyRef): Unit = {
      val hashedValue = System.identityHashCode(value)
      val bucket = extractBitRange(hashedValue, 0, k - 1)
      estimators.update(
        bucket,
        Math.max(estimators(bucket), numLeadingZeroBits(extractBitRange(hashedValue, k + 1, H))))
    }
    def +=(value: AnyRef): Unit = add(value)

    // $$ (m \int_0^\inf (\log_2 ((2+u)/(1+u))^m du) ^{-1} \\approx $$
    private val alpha = if (k <= 4) 0.673 else if (k == 5) 0.697 else if (k == 6) 0.709 else 0.7213 / (1.0 + 1.079 / m)
    def estimate: Double = {
      val V = estimators.count(_ == 0)
      if (V > 0)
        m * Math.log(m.toDouble / V) / Math.log(2.0)
      else
        alpha * m * m / estimators.map(x => Math.pow(2.0, -x)).sum
    }

    private def extractBitRange(number: Int, start: Int, end: Int): Int =
      ((1 << (end - start + 1)) - 1) & (number >> start)

    private def numLeadingZeroBits(value: Int): Int = {
      if (value == 0) 0
      else {
        var i = 0
        var x = value
        while ((x & 1) == 0) {
          i += 1
          x >>= 1
        }
        i
      }
    }
  }

  private final case class FamilyMember(
      hash: Int,
      tsk: PNodeTask,
      name: String,
      i: Int,
      ancestor2Distance: JIdentityHashMap[PNodeTask, Int],
      sortedAncestorTasks: IndexedSeq[PNodeTask],
      sortedAncestorNames: IndexedSeq[String]
  )

  private def overlap(t1: FamilyMember, c: PNodeTask, t2: FamilyMember, pair: (Int, Int)): Option[(Int, (Int, Int))] = {
    //  ancestor:       |------------------|
    //  children:  -----------|   |-----------
    val n1 = t1.tsk
    val n2 = t2.tsk
    if (
      c.firstStartTime < n1.completedTime &&
      n1.completedTime < n2.firstStartTime &&
      n2.firstStartTime < c.completedTime
    )
      Some(t2.ancestor2Distance.get(c), pair)
    else
      None
  }

  private def fold[T, Z, R](initTsk: T)(z: Z)(getRelatives: (Z, T) => Seq[T])(before: (Z, T) => Z)(after: (Z, T) => Z)(
      finish: Z => R): Z = {
    val ihm = new JIdentityHashMap[T, T]()

    var acc = z

    def visit(tsk: T): Unit = {
      if (ihm.containsKey(tsk)) {
        ihm.put(tsk, initTsk)
        acc = before(acc, tsk)
        getRelatives(acc, tsk).foreach(visit)
        acc = after(acc, tsk)
      }
    }

    try {
      visit(initTsk)
    } catch {
      case e: Exception => System.err.println(e.getMessage)
    }
    acc
  }

  private def compareTargets(
      commonNameCounts: mutable.HashMap[String, LogLogCounter],
      commonNodes: JIdentityHashMap[PNodeTask, Set[(Int, Int)]],
      getName: PNodeTask => String,
      targets: IndexedSeq[FamilyMember],
      lock: Object,
      requireArgMatch: Boolean,
      exactMatch: Boolean,
      i: Int,
      j: Int): Unit = {
    val t1 = targets(i)
    val t2 = targets(j)
    if (exactMatch) {
      var a: PNodeTask = null
      var distMin = Int.MaxValue
      var pair: (Int, Int) = null
      val pair12 = (i, j)
      val pair21 = (j, i)
      t1.ancestor2Distance.keySet.stream.filter(t2.ancestor2Distance.containsKey(_)).forEach { x =>
        (overlap(t1, x, t2, pair12) orElse overlap(t2, x, t1, pair21)) foreach { case (d, p) =>
          if (d < distMin) {
            distMin = d
            a = x
            pair = p
          }
        }
      }
      if (a ne null) {
        lock.synchronized[Unit] {
          commonNodes.put(
            a,
            Option(commonNodes.get(a)) match {
              case Some(s) => s + pair
              case None    => Set(pair)
            })
        }
      }
    } else if (
      t1.hash == t2.hash &&
      (!requireArgMatch || JArrays.equals(t1.tsk.args, t2.tsk.args)) &&
      t1.sortedAncestorTasks.size == t2.sortedAncestorTasks.size
    ) {
      // Look for common ancestor along congruent paths - i.e. the sorted ancestor names match.
      val found: Option[Int] = (1 until t1.sortedAncestorTasks.size).find { k =>
        t1.sortedAncestorNames(k) == t2.sortedAncestorNames(k) &&
        t1.sortedAncestorTasks(k).isInstanceOf[PNodeTask] && t2
          .sortedAncestorTasks(k)
          .isInstanceOf[PNodeTask] &&
        JArrays.equals(t1.sortedAncestorTasks(k).args, t2.sortedAncestorTasks(k).args)
      }
      found.foreach { k =>
        val t = t1.sortedAncestorTasks(k)
        val name = getName(t)
        lock.synchronized[Unit] {
          commonNodes.put(t, Set.empty)
          commonNodes.put(t2.sortedAncestorTasks(k), Set.empty)
          commonNameCounts.get(name) match {
            case Some(b) =>
              b += t1
              b += t2
            case None =>
              val c = new LogLogCounter(8)
              c += t1
              c += t2
              commonNameCounts += name -> c
          }
        }
      }
    }
  }

  /**
   * Find closest common ancestors. Returns map of ancestor name to approximate count of descendant leaves, and full set
   * of common ancestor tasks.
   *
   * @param tsks
   *   Leaf tasks
   * @param notify
   *   Write to console every notify rows
   * @param requireArgsMatch
   *   Only match leaves with matching args
   * @param maxLevel
   *   Limit ancestor search
   */
  @entersGraph def closestCommonAncestors(
      tsks: Iterable[PNodeTask],
      notify: Int = 10000,
      requireArgsMatch: Boolean = false,
      exact: Boolean = false,
      maxLevel: Int = 100): (Map[String, Long], JIdentityHashMap[PNodeTask, Set[(Int, Int)]]) = {
    GraphInputConfiguration.setTraceMode("none")
    val names = new ConcurrentHashMap[PNodeTask, String]()

    def getName(t: PNodeTask) = names.computeIfAbsent(t, _.toPrettyName(false, false))

    type T = (Int, String, PNodeTask)
    // Sort ancestors first by degree, then by name, finally disambiguate by hash if necessary
    object c extends Comparator[T] {
      override def compare(o1: T, o2: T): Int = {
        var ret = o1._1.compareTo(o2._1)
        if (ret != 0) return ret
        ret = o1._2.compareTo(o2._2)
        if (ret != 0) return ret
        System.identityHashCode(o1._3).compareTo(System.identityHashCode(o2._3))
      }
    }
    val targets: IndexedSeq[FamilyMember] = tsks.zipWithIndex
      .map { case (tsk, index) =>
        val am = new JIdentityHashMap[PNodeTask, Int]
        val as = ArrayBuffer.empty[T]
        foreachAncestor(tsk, identity, after = false, maxLevel) {
          case (p: PNodeTask, j) =>
            as += ((j, getName(p), p))
            am.put(p, j)
          case _ =>
        }
        val ar: Array[T] = as.toArray
        // Sort ancestors in place
        JArrays.sort[T](ar, c)
        val sorted = new Array[PNodeTask](ar.length)
        val sortedNames = new Array[String](ar.length)
        for (i <- ar.indices) {
          sorted(i) = ar(i)._3
          sortedNames(i) = ar(i)._2
        }
        val hash = sortedNames.toSeq.hashCode
        FamilyMember(hash, tsk, getName(tsk), index, am, sorted, sortedNames)
      }
      .toArray
      .toIndexedSeq
    if (notify > 0) println("Prepared targets")
    val commons = mutable.HashMap.empty[String, LogLogCounter]
    val commonNodes = new JIdentityHashMap[PNodeTask, Set[(Int, Int)]]
    val lock = new Object
    val n = targets.size
    (0 until n).foreach { i =>
      var j = i + 1
      while (j < n) {
        compareTargets(commons, commonNodes, getName, targets, lock, requireArgsMatch, exact, i, j)
        j += 1
      }
      if (notify > 0 && i % notify == 0) println(s"i=$i")
    }
    if (notify > 0) println("Done correlating")
    (
      commons.map { case (k, b) =>
        (k, b.estimate.toLong)
      }.toMap,
      commonNodes)
  }
}
