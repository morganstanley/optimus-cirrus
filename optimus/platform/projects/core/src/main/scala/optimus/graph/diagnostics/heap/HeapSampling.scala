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
package optimus.graph.diagnostics.heap
import optimus.core.HeapDumper
import optimus.graph.DiagnosticSettings
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.Date
import java.util.Timer
import java.util.TimerTask
import optimus.breadcrumbs.ChainedID

final case class HeapSamplingSettings(
    minDumpInterval: Duration,
    avgDumpInterval: Duration,
    maxHprofProduced: Int
)

object HeapSamplingSettings {
  // valid keys
  private val MinDumpInterval = "min"
  private val AvgDumpInterval = "every"
  private val MaxHprofProduced = "maxNumber"
  // defaults
  val default: HeapSamplingSettings = HeapSamplingSettings(Duration.ofMinutes(1L), Duration.ofMinutes(10L), 3)

  private[diagnostics] val log = LoggerFactory.getLogger(HeapSamplingSettings.getClass)

  private def parseDuration(duration: String) = try { Some(Duration.parse(duration)) }
  catch {
    case _: DateTimeParseException =>
      log.warn(s"Unable to parse ${duration}. Did you use a valid ISO 8601 duration?")
      None
  }

  private def parseInt(int: String) = try { Some(int.toInt) }
  catch {
    case _: NumberFormatException =>
      log.warn(s"Unable to parse ${int} as an integer.")
      None
  }

  private def splitPair(s: String) =
    s.splitAt(s.indexOf('=')) match {
      case ("", key)    => key -> ""
      case (key, value) => key -> value.drop(1) // remove the leading =
    }

  private def parseKv(kv: Map[String, String]) = {
    val (result, unrecognized) = kv.foldLeft((default, Seq.empty[String])) {
      case ((settings, unrecognized), (key, value)) =>
        val updated = key match {
          case MinDumpInterval  => parseDuration(value).map(d => settings.copy(minDumpInterval = d))
          case AvgDumpInterval  => parseDuration(value).map(d => settings.copy(avgDumpInterval = d))
          case MaxHprofProduced => parseInt(value).map(d => settings.copy(maxHprofProduced = d))
          case _                => None
        }

        updated match {
          case Some(updated) => (updated, unrecognized)
          case None          => (settings, unrecognized :+ key)
        }
    }

    unrecognized.foreach(k => log.warn(s"Unrecognized key ${k} in heap profiler settings."))
    result
  }

  def apply(s: String): Option[HeapSamplingSettings] = {
    Option(s)
      .filterNot(_ == "false")
      .map(_.split(':').map(splitPair).toMap)
      .map(parseKv)
  }
}

object HeapSampling {
  private[diagnostics] val log = LoggerFactory.getLogger(HeapSampling.getClass)
  private val dumpDir = Paths.get(DiagnosticSettings.heapProfileDir)
  private val settings = HeapSamplingSettings(DiagnosticSettings.heapProfileSettings)

  private var lastHeapDump = Instant.EPOCH
  private var timer: Timer = null
  private var remainingAllowedDumps = 0 // decremented in a synchronized block

  private def stopTimer() = this.synchronized {
    if (timer != null) {
      timer.cancel()
      timer = null
    }
  }

  private def generateFilename(when: Instant): String =
    ChainedID.root.toString + "_" + DateTimeFormatter
      .ofPattern("yyyy-MM-dd_HH-mm-ss")
      .withZone(ZoneId.systemDefault())
      .format(when)

  private def dumpTheHeap(settings: HeapSamplingSettings) = synchronized {
    if (remainingAllowedDumps > 0) {
      val now = Instant.now()
      if (Duration.between(lastHeapDump, now).compareTo(settings.minDumpInterval) > 0) {
        val hprofFile = dumpDir.resolve(generateFilename(now) + ".hprof").toAbsolutePath.toString
        log.info("sampling: " + hprofFile)
        Files.createDirectories(dumpDir)
        try {
          HeapDumper.dumpHeap(hprofFile, false)
          log.info("done")
        } catch {
          case exc: Throwable => log.warn("heap dump failed", exc)
        } finally {
          lastHeapDump = now
          remainingAllowedDumps -= 1
        }
      }
    } else {
      log.info("not dumping the heap: maximum number of heap dumps was reached")
    }
  }

  def initialize(): Unit = settings.foreach { s =>
    remainingAllowedDumps = s.maxHprofProduced
    log.info(
      s"heap will be dumped in ${dumpDir.toString} with frequency ${s.avgDumpInterval.toString} a maximum of ${remainingAllowedDumps} times")
    stopTimer()
    timer = new Timer("HeapSampling timer", true)
    timer.schedule(
      new TimerTask { override def run(): Unit = dumpTheHeap(s) },
      Date.from(Instant.now().plus(s.avgDumpInterval)),
      s.avgDumpInterval.toMillis)
  }

  def exiting(): Unit = {
    stopTimer()
    settings.foreach(dumpTheHeap)
  }

}
