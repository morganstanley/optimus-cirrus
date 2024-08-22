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

import optimus.platform.util.Log
import optimus.platform.util.ServiceLoaderUtils
import optimus.utils.StringPredicate

sealed trait SampledTimers {
  def samples: Map[String, Long]
}

trait FrameMatcherProvider extends Log {
  def frameMatchers: Seq[SampledTimersExtractor.FrameMatcher]
}

object DefaultFrameMatchers extends FrameMatcherProvider {
  import optimus.utils.StringTyping._
  private val samplingAndAp =
    startsWith("optimus/graph/diagnostics/ap") or startsWith("optimus/graph/diagnostics/sampling")
  private val instrumentation = startsWith("java/lang/instrument")
  override def frameMatchers: Seq[SampledTimersExtractor.FrameMatcher] = Seq(
    SampledTimersExtractor.matcher("samplingOH", samplingAndAp),
    SampledTimersExtractor.matcher("instrumOH", instrumentation)
  )
}

object SampledTimersExtractor extends Log {

  final private class MutableRecordingState(analyers: IndexedSeq[FrameMatcher]) extends SampledTimers {
    var everything = 0L
    val counts = Array.fill(analyers.length)(0L)
    override def samples: Map[String, Long] =
      analyers.zip(counts).map { case (a, c) => a.name -> c }.toMap + ("everything" -> everything)
  }

  private val DONE = -1

  /**
   * Update a count for every stack that matches the ordered sequence of predicates in `pred`.
   */
  final case class FrameMatcher(name: String, pred: Array[StringPredicate]) {
    def test(frame: String, step: Int): Int = {
      if (pred(step).test(frame)) {
        if (step == pred.length - 1) DONE
        else step + 1
      } else step
    }
    override def toString: String = s"FrameMatcher($name, ${pred.mkString(", ")})"
  }

  def matcher(name: String, preds: StringPredicate*): FrameMatcher = FrameMatcher(name, preds.toArray)

  private val analysers = (ServiceLoaderUtils
    .all[FrameMatcherProvider]
    .flatMap(_.frameMatchers) ++ DefaultFrameMatchers.frameMatchers).toArray

  log.info(s"Installing analysers: ${analysers.mkString(", ")}")

  def newRecording: SampledTimers = new MutableRecordingState(analysers)

  def analyse(rec: SampledTimers, frames: Array[String], count: Long): Unit = {
    val state = Array.fill(analysers.length)(0)
    val mut = rec match { case mut: MutableRecordingState => mut }
    mut.everything += count
    for (f <- frames) {
      for (i <- analysers.indices) {
        if (state(i) >= 0) {
          state(i) = analysers(i).test(f, state(i))
          if (state(i) == DONE) { mut.counts(i) += count }
        }
      }
    }
  }
}
