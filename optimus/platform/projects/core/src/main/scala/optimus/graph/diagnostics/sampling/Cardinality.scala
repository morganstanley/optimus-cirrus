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
package optimus.graph.diagnostics.sampling
import optimus.platform.util.Log

object Cardinality extends Log {

  class LogLogCounter private (val estimators: Array[Int], var withDuplicates: Long) {
    private val numHashBits = 64
    private val m = estimators.size // had better be power of 2
    private val k = Integer.numberOfTrailingZeros(m)
    assert(m == 1 << k)

    def this(buckets: Int) = this(new Array[Int](1 << Math.round(Math.log(buckets) / Math.log(2)).toInt), 0L)

    def add(hashedValue: Long): Unit = {
      withDuplicates += 1
      val bucket = extractBitRange(hashedValue, 0, k - 1)
      val zeroes = Integer.numberOfTrailingZeros(extractBitRange(hashedValue, k + 1, numHashBits)) + 1
      estimators.update(bucket, Math.max(estimators(bucket), zeroes))
    }

    private def extractBitRange(number: Long, start: Int, end: Int): Int =
      (((1 << (end - start + 1)) - 1) & (number >> start)).toInt

    def add(other: LogLogCounter): Unit = {
      assert(m == other.m)
      (0 until m).foreach { i =>
        estimators(i) = Math.max(estimators(i), other.estimators(i))
      }
      withDuplicates += other.withDuplicates
    }

    def snap = new LogLogCounter(estimators.clone(), withDuplicates)

    private def log2(x: Double) = Math.log(x) / Math.log(2.0)
    val alpha: Double = if (k <= 4) 0.673 else if (k == 5) 0.697 else if (k == 6) 0.709 else 0.7213 / (1.0 + 1.079 / m)
    val numerator = alpha * m * m

    // See https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
    // and https://en.wikipedia.org/wiki/HyperLogLog
    def estimate: Double = {
      val E = alpha * m * m / estimators.map(x => Math.pow(2.0, -x)).sum
      if (E > 2.5 * m)
        E
      else {
        val V = estimators.count(_ == 0)
        if (V > 0)
          m * log2(m.toDouble / V)
        else
          E
      }
    }
  }

  def union(e1: Array[Int], e2: Array[Int]): Array[Int] = {
    val m = e1.size
    assert(m == e2.size)
    e1.zip(e2).map { case (v1, v2) =>
      Math.max(v1, v2)
    }
  }

  sealed trait Category {
    def name: String
  }

  object versionedEntityRef extends Category {
    override val name = "vref"
  }

  // for testing purposes
  object stringCard extends Category {
    override val name = "strTest"
  }

  import optimus.scalacompat.collection._

  class Counters(val countEstimateMap: Map[Category, LogLogCounter]) {
    // to be expanded when we add additional categories
    def this() = this(Map(versionedEntityRef -> new LogLogCounter(32)))
    def add(cat: Category, hash: Long): Unit = if (countEstimateMap.contains(cat)) countEstimateMap(cat).add(hash)
    else log.warn(s"Category $cat not present in the counters")
    def snap: Counters = new Counters(countEstimateMap.mapValuesNow(_.snap))
    def add(other: Counters) = countEstimateMap.foreach { case (cat, counter) =>
      if (other.countEstimateMap.contains(cat)) counter.add(other.countEstimateMap(cat))
    }
    def cardinalities: Map[Category, Double] = countEstimateMap.mapValuesNow(_.estimate)
  }
}
