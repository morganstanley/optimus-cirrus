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
package optimus.buildtool.trace

import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.util.Timer
import java.util.TimerTask

import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.app.OptimusBuildToolBootstrap
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.graph.diagnostics.DefensiveManagementFactory
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.annotation.nowarn
import scala.io.Source

object TimingsRecorder {

  private[TimingsRecorder] final case class TaskGroup(start: Double, duration: Double)

  private val log = getLogger(getClass)

  // we don't do all tasks and we group many of them to reduce output size
  def jsCategorize(scope: ScopeId, cat: CategoryTrace): Option[String] = {
    val id = scope match {
      case ScopeId.RootScopeId                => "root" // this is used in timings.js to determine the color
      case ScopeId(meta, bundle, module, tpe) => s"${meta.charAt(0)}.${bundle.charAt(0)}.${module}.${tpe}"
    }

    Option(cat match {
      case Scala | Java | Cpp | Signatures | Outline => (id, "compile")
      case PostInstallApp(_)                         => (id, "postinstall")
      case Upload(_, _)                              => (id, "upload")
      case _                                         => null // we don't save all the operations
    })
      .map { case (id, cat) =>
        s"${id}:${cat}"
      }
  }

  private[TimingsRecorder] final case class TimingsTraceResult(
      i: Long,
      name: String,
      start: Double,
      duration: Double,
      signature_time: Option[Double],
      unlocked_units: Seq[Long],
      unlocked_signature_units: Seq[Long])

  private[TimingsRecorder] implicit val TimingsTraceResultJsonFormat: RootJsonFormat[TimingsTraceResult] = jsonFormat7(
    TimingsTraceResult.apply)

  private[TimingsRecorder] final case class ConcurrencyResult(t: Double, active: Long, waiting: Long, blocked: Long)

  private[TimingsRecorder] implicit val ConcurrencyResultJsonFormat: RootJsonFormat[ConcurrencyResult] = jsonFormat4(
    ConcurrencyResult.apply)

  private def toEpocMicro(i: Instant): Long =
    (i.getEpochSecond * 1000000L) + (i.getNano / 1000L)

  private[trace] class TimingsRecorderTask(val scopeId: ScopeId, val category: CategoryTrace, startTime: Instant)
      extends DefaultTaskTrace {
    val startTimeMicros: Long = toEpocMicro(startTime)
    def endTimeMicros = endTimeMicrosVar
    @volatile private var endTimeMicrosVar: Long = -1
    def isDone: Boolean = endTimeMicros > -1

    override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit = synchronized {
      assert(!isDone, s"task was already completed: $this ")
      endTimeMicrosVar = toEpocMicro(time)
    }
  }

  // a very simple, low-fidelity sampler of resources
  private val samplingInterval = 500 // in millis
  private[TimingsRecorder] final case class Snap(
      timeMicro: Long,
      `%cpu`: Double
  )

  private class LowFiSampler {
    var snapTimer: Timer = null
    protected[trace] val traces = List.newBuilder[Snap]

    private def snapTimerTask(): TimerTask = new TimerTask() {
      val osBean = DefensiveManagementFactory.getOperatingSystemMXBean()

      override def run(): Unit = {
        @nowarn("cat=deprecation") val load = osBean.getSystemCpuLoad()
        // could add memory usage etc here
        traces += Snap(toEpocMicro(Instant.now()), load * 100.0)
      }
    }

    def snaps: List[Snap] = traces.result()

    def start(): Unit = {
      stop()
      snapTimer = new Timer(true)
      snapTimer.schedule(snapTimerTask(), 100L, samplingInterval)
    }

    def stop(): Unit = {
      if (snapTimer != null) {
        snapTimer.cancel()
        snapTimer.purge()
        snapTimer = null
      }
    }

    def reset(): Unit = {
      stop()
      traces.synchronized { traces.clear() }
      start()
    }
  }

}

class TimingsRecorder(outputTo: Directory) extends DefaultObtTraceListener {
  import TimingsRecorder._
  // start time
  private var _timingsStart: Instant = null
  private def timingsStart: Instant = if (_timingsStart == null) {
    throw new RuntimeException("Timings recorder build has ended without a corresponding call to .startBuild()!")
  } else _timingsStart

  // cpu sampler
  private val sampler = new LowFiSampler()

  override def startBuild(): Unit = {
    traces.clear()
    _timingsStart = Instant.now()
    sampler.start()
  }

  // actual jobs
  protected[trace] val traces = List.newBuilder[TimingsRecorderTask]

  override def startTask(
      scopeId: ScopeId,
      category: CategoryTrace,
      time: Instant = patch.MilliInstant.now()): DefaultTaskTrace = {
    category match {
      case SilverkingOperation(_, _) => NoOpTaskTrace
      case _ => {
        val t = new TimingsRecorderTask(scopeId, category, time)
        traces.synchronized {
          traces += t
        }
        t
      }
    }
  }

  private def completedTasks: List[TimingsRecorderTask] = traces.synchronized { traces.result() }.filter(_.isDone)

  private def getSamplerJsData(timingsEnd: Instant): String = {
    def tween(start: Long, end: Long): Double = (end - start) * 1e-6
    val globalStart = toEpocMicro(timingsStart)
    val globalEnd = toEpocMicro(timingsEnd)
    val snaps = sampler.snaps.filter(snap => snap.timeMicro > globalStart && snap.timeMicro < globalEnd)
    log.debug(s"took ${snaps.size} snapshots")

    snaps.map(s => (tween(globalStart, s.timeMicro), s.`%cpu`)).toJson.toString
  }

  private def getJsData(timingsEnd: Instant): (String, Long) = {
    def tween(start: Long, end: Long): Double = ((end - start) * 1e-5).round * 1e-1 // round to tenth of sec

    val tasks = completedTasks
    val globalStart = toEpocMicro(timingsStart)
    val globalEnd = toEpocMicro(timingsEnd)

    val buildDuration = tween(globalStart, globalEnd)

    log.debug(s"${tasks.size} completed tasks")

    val jsonData = tasks
      .groupBy(task => jsCategorize(task.scopeId, task.category))
      .collect {
        case (Some(jsCat), tasks) => {
          val groupStart = tasks.map(_.startTimeMicros).min
          val groupEnd = tasks.map(_.endTimeMicros).max
          val signatures = tasks.filter {
            _.category match {
              case Signatures | Outline => true
              case _                    => false
            }
          }
          val sigTime = if (signatures.size > 0) Some(signatures.map(_.endTimeMicros).max) else None

          TimingsTraceResult(
            i = 0, // we update this later
            name = jsCat,
            start = tween(globalStart, groupStart),
            duration = tween(groupStart, groupEnd),
            signature_time = sigTime.map(x => tween(groupStart, x)),
            unlocked_units = Seq.empty,
            unlocked_signature_units = Seq.empty
          )
        }
      }
      .toSeq
      .sortBy(_.start)
      .zipWithIndex
      .map { case (result, index) =>
        result.copy(i = index)
      }
      .toJson
      .toString

    (jsonData, buildDuration.ceil.round)
  }

  def buildReport: String = {
    log.debug("building timings report")
    def loadResource(path: String): String = {
      val s = Source.fromResource(path)
      try s.mkString
      finally s.close()
    }

    val template = loadResource("optimus.buildtool.trace/template.html")
    val jsTiming = loadResource("optimus.buildtool.trace/timings.js")

    val end = Instant.now()
    val (jsUnitData, buildDuration) = getJsData(end)
    val jsCpuUsage = getSamplerJsData(end)

    val beginJs = s"""
                     |<script>
                     |DURATION = ${buildDuration};""".stripMargin
    val jsData = s"""|
                     |const UNIT_DATA = ${jsUnitData};
                     |const CPU_USAGE = ${jsCpuUsage};
                     |""".stripMargin
    val endJs = """
                  |</script>
                  |</body>
                  |</html>""".stripMargin

    template.mkString.replace("{ROOTS}", "some scope or something") ++ beginJs ++ jsData ++ jsTiming.mkString ++ endJs
  }

  def writeReport(): Unit = {}

  override def endBuild(success: Boolean): Boolean = {
    val resultPath = outputTo.resolveFile(s"obt-timings-${OptimusBuildToolBootstrap.logFilePrefix}.html").path
    log.info(s"Writing timings report to ${resultPath.toAbsolutePath.toString}")
    Files.writeString(
      resultPath,
      buildReport,
      StandardOpenOption.WRITE,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
    _timingsStart = null
    true // this recorder is always satisfied with builds
  }
}
