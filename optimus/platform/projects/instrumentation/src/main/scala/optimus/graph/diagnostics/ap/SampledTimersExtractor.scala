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
package optimus.graph.diagnostics.ap

import optimus.graph.diagnostics.ap.SampledTimersExtractor.FrameMatcher
import optimus.graph.diagnostics.ap.StackAnalysis.BackedArray
import optimus.graph.diagnostics.ap.StackAnalysis.CleanName
import optimus.platform.util.Log
import optimus.platform.util.ServiceLoaderUtils

import java.util.function.Predicate
import scala.collection.mutable

sealed trait SampledTimers {
  def samples: Map[String, Long]
}

trait FrameMatcherProvider extends Log {
  def frameMatchers: Seq[SampledTimersExtractor.FrameMatcher]
  protected def matcher(name: String, preds: StringPredicate*): FrameMatcher = FrameMatcher(name, preds.toArray)
}

object StringPredicate {
  private val all = mutable.ArrayBuffer.empty[StringPredicate]
  def startsWith(prefix: String): StringPredicate =
    new StringPredicate(s"startsWith($prefix)", (t: String) => t.startsWith(prefix))
  def contains(substr: String): StringPredicate =
    new StringPredicate(s"contains($substr)", (t: String) => t.contains(substr))
  def isEqualTo(value: String): StringPredicate = new StringPredicate(s"isEqualTo($value)", (t: String) => t == value)

  def getMaskForAllPredicates(s: String): Long = synchronized {
    var bits = 0L
    all.indices.foreach { i =>
      val sp: StringPredicate = all(i)
      if (sp.pred.test(s))
        bits |= sp.mask
    }
    bits
  }
}

final class StringPredicate private (name: String, val pred: Predicate[String]) {
  private var mask = 0L
  // We don't assign a mask to components of compound predicates
  private[ap] def assignMask(): Long = StringPredicate.synchronized {
    if (mask == 0) {
      import StringPredicate.all
      all += this
      assert(all.size < 64, "Maximum number of frame matcher predicates exceeded!")
      mask = 1 << all.size
    }
    mask
  }

  def test(x: Long): Boolean = (mask & x) != 0L

  override def toString: String = name
  def and(other: StringPredicate): StringPredicate = {
    val outer = this
    new StringPredicate(s"($this and $other)", (t: String) => outer.pred.test(t) && other.pred.test(t))
  }
  def or(other: StringPredicate): StringPredicate = {
    val outer = this
    new StringPredicate(s"($this or $other)", (t: String) => outer.pred.test(t) || other.pred.test(t))
  }
  def negate(): StringPredicate = {
    val outer = this
    new StringPredicate(s"!$this", (t: String) => !outer.pred.test(t))
  }
}

object DefaultFrameMatchers extends FrameMatcherProvider {
  import StringPredicate._
  private val samplingAndAp =
    startsWith("optimus/graph/diagnostics/ap") or startsWith("optimus/graph/diagnostics/sampling")
  private val instrumentation = startsWith("java/lang/instrument")
  private val skipped = startsWith("[java_skipped")
  override def frameMatchers: Seq[SampledTimersExtractor.FrameMatcher] = Seq(
    matcher("samplingOH", samplingAndAp),
    matcher("instrumOH", instrumentation),
    matcher("javaSkipped", skipped)
  )
}

object SampledTimersExtractor extends Log {

  final private class MutableRecordingState(analyers: IndexedSeq[FrameMatcher]) extends SampledTimers {
    var everything = 0L
    val counts = new Array[Long](analyers.length)
    override def samples: Map[String, Long] =
      analyers.zip(counts).map { case (a, c) => a.name -> c }.toMap + ("everything" -> everything)
  }

  private val MATCHED = -1

  /**
   * Update a count for every stack that matches the ordered sequence of predicates in `pred`.
   */
  final case class FrameMatcher private[ap] (name: String, preds: Array[StringPredicate]) {
    val mask = preds.map(_.assignMask()).reduce(_ | _)
    def testAndAdvanceState(frame: CleanName, step: Int): Int = {
      if (preds(step).test(frame.predMask)) {
        if (step == preds.length - 1) MATCHED
        else step + 1
      } else step
    }
    override def toString: String = s"FrameMatcher($name, ${preds.mkString(", ")})"
  }

  final private case class AllAnalysers(frameMatchers: Array[FrameMatcher], mask: Long)

  lazy private val analysers: AllAnalysers = {
    val as = (ServiceLoaderUtils
      .all[FrameMatcherProvider]
      .flatMap(_.frameMatchers) ++ DefaultFrameMatchers.frameMatchers).toArray
    log.info(s"Installing analysers: ${as.mkString(", ")}")
    val mask = as.map(_.mask).reduce(_ | _)
    AllAnalysers(as, mask)
  }

  def newRecording: SampledTimers = new MutableRecordingState(analysers.frameMatchers)

  def analyse(rec: SampledTimers, frames: BackedArray[CleanName], count: Long): Unit = {
    val state = new Array[Int](analysers.frameMatchers.length)
    val mut = rec match { case mut: MutableRecordingState => mut }
    val frameMatchers = analysers.frameMatchers
    val matchAny = analysers.mask
    mut.everything += count
    for (f <- frames) {
      // Don't bother checking if no known predicates match
      if ((f.predMask & matchAny) != 0)
        for (i <- frameMatchers.indices) {
          if (state(i) >= 0) {
            state(i) = frameMatchers(i).testAndAdvanceState(f, state(i))
            if (state(i) == MATCHED) { mut.counts(i) += count }
          }
        }
    }
  }
}
