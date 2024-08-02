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

import java.util.function.Predicate

object SampledTimersExtractor extends TimerExtractor {
  sealed trait Recording {
    def result(): SampledTimers
  }
}

sealed abstract class SampledTimers {
  // All counts, useful as a normalization
  def everything: Long

  // All graph work, like profGraphCpuTime
  def graphTime: Long

  def syncStackTime: Long

  // All cache lookups, like profCacheTime, but cpu-based
  def cacheTime: Long

  // Async and sync tweak resolution
  def tweakLookupAsync: Long
  def tweakLookupSync: Long

  // overheads
  def overheadSampling: Long
  def overheadInstrum: Long

  def localTables: Long
}

trait TimerExtractor {
  import SampledTimersExtractor._
  import TimerExtractor._

  private val ogscRun: Predicate[String] = "optimus/graph/OGSchedulerContext.run".equals
  private val ogscRunAndWait: Predicate[String] = "optimus/graph/OGSchedulerContext.runAndWait".equals
  private val ncCacheLookup: Predicate[String] = _.startsWith("optimus/graph/cache/NodeCache$.cacheLookup")
  private val twkAsyncResolve: Predicate[String] = "optimus/graph/TwkResolver.asyncResolve".equals
  private val twkSyncResolve: Predicate[String] = "optimus/graph/TwkResolver.syncResolve".equals
  private val samplingAndAp: Predicate[String] = (f: String) =>
    f.startsWith("optimus/graph/diagnostics/ap") || f.startsWith("optimus/graph/diagnostics/sampling")
  private val instrumentation: Predicate[String] = _.startsWith("java/lang/instrument")
  private val localTables: Predicate[String] = _.startsWith("optimus/graph/OGLocalTables.forAllRemovables")

  private val analysers = Array(
    matcher(ogscRun or ogscRunAndWait)(_.graphTime += _),
    // looking for a run() followed by a runAndWait()
    matcher(ogscRun or ogscRunAndWait, ogscRunAndWait)(_.syncStackTime += _),
    matcher(ncCacheLookup)(_.cacheTime += _),
    matcher(twkAsyncResolve)(_.tweakLookupAsync += _),
    matcher(twkSyncResolve)(_.tweakLookupSync += _),
    matcher(samplingAndAp)(_.overheadSampling += _),
    matcher(instrumentation)(_.overheadInstrum += _),
    matcher(localTables)(_.localTables += _)
  )

  def newRecording: Recording = new MutableRecordingState

  def analyse(rec: Recording, frames: Array[String], count: Long): Unit = {
    val state = Array.fill(analysers.length)(0)
    val mut = rec match { case mut: MutableRecordingState => mut }
    mut.everything += count

    for (f <- frames) {
      for (i <- analysers.indices) {
        if (state(i) >= 0) {
          state(i) = analysers(i).test(f, state(i))
          if (state(i) == DONE) { analysers(i).update(mut, count) }
        }
      }
    }
  }
}

object TimerExtractor {
  import SampledTimersExtractor.Recording

  private final class MutableRecordingState extends SampledTimers with Recording {
    var everything = 0L

    var graphTime = 0L
    var syncStackTime = 0L

    var cacheTime = 0L
    var tweakLookupAsync = 0L
    var tweakLookupSync = 0L

    var overheadSampling = 0L
    var overheadInstrum = 0L

    var localTables = 0L

    override def result(): SampledTimers = this
  }

  private type Update = (MutableRecordingState, Long) => Unit
  private val DONE = -1

  /**
   * Update a count for every stack that matches the ordered sequence of predicates in `pred`.
   */
  final private case class FrameMatcher(pred: Array[Predicate[String]], update: Update) {
    def test(frame: String, step: Int): Int = {
      if (pred(step).test(frame)) {
        if (step == pred.length - 1) DONE
        else step + 1
      } else step
    }
  }

  private def matcher(preds: Predicate[String]*)(update: Update) = FrameMatcher(preds.toArray, update)
}
