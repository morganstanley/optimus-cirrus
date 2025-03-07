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

package optimus.utils
import java.util.concurrent.ThreadLocalRandom
import scala.{util => sutil}

/**
 * Keeps track of a histogram using only 32 bits
 * Each bucket is allocated bucketDepthBits of storage; together those bits represent log_2 of the number of elements in the bucket, so have 32/bucketDepthBits buckets.
 * Each bucket is 2^bucketWidthBits wide.
 */
class LogLogHistogram(bucketWidthBits: Int, bucketDepthBits: Int, totalBits: Int = 64) {

  val numBuckets: Int = totalBits / bucketDepthBits
  val maxLogVal: Int = (1 << bucketDepthBits) - 1
  val maxLogValL: Long = maxLogVal

  def bits(n: Long): String = n.toBinaryString

  def xAxis: Seq[Long] =
    (1 to numBuckets).map(i => 1L << (i * bucketWidthBits))

  def yAxis(reg: Long): Seq[Long] =
    (0 until numBuckets).map { i =>
      val v = (reg >> (i * bucketDepthBits)) & maxLogVal
      if (v == 0) 0
      else 1L << v
    }

  final def add(reg: Long, sample: Int, logRandom: Log2Random): Long = {
    // Bucket is the log base bucket-width of the sample. I.e. if bucket width is 2,
    // then a sample of 0-3 is in the first bucket, 4-15 is in the 2nd, etc.
    val iBucket = Math.min((31 - Integer.numberOfLeadingZeros(sample)) / bucketWidthBits, numBuckets - 1)
    val b = iBucket * bucketDepthBits
    val currentLogVal = ((reg >> b) & maxLogVal).toInt
    // We're accumulating lc === the smallest power of 2 that is greater than the count
    // If the lc==0, we always increment, effectively to count=2
    // If lc==1, there's a 50% chance that we increment to lc=2, count=4
    // If lc==2, there's a 25% chance that we increment to lc=3, count=8
    if (currentLogVal == maxLogVal)
      reg
    else if (currentLogVal > 0 && logRandom.fewerZeros(currentLogVal))
      reg
    else {
      (reg & ~(maxLogValL << b)) | ((currentLogVal + 1L) << b)
    }
  }

  // Combine two histograms, bucket by bucket.
  // This has to be probabilistic.  If you combine 32 into 128 4 times you'll get 256; on
  // average we'll be right if we say that there's a 25% chance that combining it once yields 256.
  def combine(reg1: Long, reg2: Long): Long = if (reg1 == 0 && reg2 == 0) 0
  else {
    var ret = 0L
    (0 until numBuckets).foreach { i =>
      val shift = i * bucketDepthBits
      val v1log2 = (reg1 >> shift) & maxLogVal
      val v2log2 = (reg2 >> shift) & maxLogVal
      val toAdd = {
        if (v1log2 == 0) v2log2
        else if (v2log2 == 0) v1log2
        else {
          val max = Math.max(v1log2, v2log2)
          val diff = Math.abs(v1log2 - v2log2)
          if (max < maxLogVal && Integer.numberOfTrailingZeros(sutil.Random.nextInt()) >= diff) max + 1
          else max
        }
      }
      ret |= (toAdd << shift)
    }
    ret
  }

  private val bars = "▁▂▃▅▆▇"

  def barChart(reg: Long): String = {
    val ys: Seq[Double] = (0 until numBuckets).map { i =>
      val logCount = ((reg >> (i * bucketDepthBits)) & maxLogVal).toInt
      if (logCount == 0) 0.0
      else
        Math.max(1.0, logCount)
    }
    val yMax: Double = ys.max + 1.0
    ys.map(y => bars(((y / yMax) * bars.length).toInt)).mkString
  }

}

/**
 * Specialized support for checking if a random deviate [0,1] < 2^(-n)
 * This is equivalent to asking if the first n bits of a random integer are zero.
 * We avoid calling nextInt() frequently by pulling off only n bits at a time.
 * Note that this is not thread-safe; an instance should be used only by one thread.
 */
final class Log2Random() {
  override def toString = reg.toBinaryString
  private var reg = 0
  def fewerZeros(n: Int): Boolean = {
    val nextReg = reg >> n
    // If we've run out of bits, load up a new integer.  Note that this introduces a slight
    // bias, since the higher bits might have been zero by chance. In practice, these means
    // that bucket counts grow slightly more slowly than they should, but the effect is tiny.
    if (nextReg == 0) {
      var nextRand = 0
      while (nextRand <= 0)
        nextRand = Math.abs(ThreadLocalRandom.current().nextInt())
      reg = nextRand >> n
      Integer.numberOfTrailingZeros(nextRand) < n
    } else {
      val ret = Integer.numberOfTrailingZeros(reg) < n
      reg = nextReg
      ret
    }
  }
}
