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

import java.time.Instant
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap

import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.{CrumbLogger => clog}
import optimus.buildtool.config.ScopeId
import optimus.logging.LoggingInfo

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

private[buildtool] object CountingTrace {
  private val log = getLogger(this.getClass)
}

private[buildtool] final class CountingTrace(statusIntervalSec: Option[Int] = None) extends DefaultObtTraceListener {
  import CountingTrace._

  private final class CountingTaskTrace(val scopeId: ScopeId, val category: CategoryTrace, startTime: Instant)
      extends DefaultTaskTrace {
    private var isDone = false

    {
      val running = changeCount(false)
      logTask(scopeId, category.name, "start", s"total running ${category.name}=$running")
    }

    final def logTask(scopeId: ScopeId, category: String, tpe: String, extraInfo: => String): Unit = {
      lazy val extraInfoEvaluated = extraInfo
      log.debug(s"$scopeId:$category:$tpe $extraInfoEvaluated")
      clog.debug(f"$scopeId:$category:$tpe $extraInfoEvaluated [${LoggingInfo.getHost}]")
    }

    override def end(success: Boolean, errors: Int, warnings: Int, time: Instant): Unit = synchronized {
      assert(!isDone, s"task was already completed: $this")
      isDone = true
      val durationInMillis = System.currentTimeMillis() - startTime.toEpochMilli
      val completion = if (success) "succeeded" else "failed"
      val running = changeCount(true)
      logTask(
        scopeId,
        category.name,
        completion,
        f"(errors: $errors, warnings: $warnings, $durationInMillis%,d ms) total running ${category.name}=$running"
      )
    }

    override def toString: String = s"CountingTaskTrace($scopeId, $category)"

    private def changeCount(remove: Boolean): Int = {
      counts
        .compute(
          category,
          (c: CategoryTrace, m: Map[ScopeId, (Int, Int)]) => {
            val map = if (m eq null) Map.empty[ScopeId, (Int, Int)] else m
            val (running, started) = map.getOrElse(scopeId, (0, 0))
            if (remove && running == 0) {
              log.error(s"Attempting to stop $c for $scopeId, which isn't running, started=$started")
              map + (scopeId -> (0, started))
            } else if (remove)
              map + (scopeId -> (running - 1, started))
            else {
              if (c.isSingleton && (running != 0 || started != 0)) {
                log.warn(s"Starting $c/$scopeId, but running=$running, started=$started")
                ObtTrace.addToStat(ObtStats.Reruns, 1)
              }
              map + (scopeId -> (running + 1, started + 1))
            }
          }
        )
        .values
        .map(_._1)
        .sum
    }
  }

  // (running, ran)
  private val counts = new ConcurrentHashMap[CategoryTrace, Map[ScopeId, (Int, Int)]]

  private val interval =
    (statusIntervalSec.filter(_ > 0) orElse Option(System.getProperty("optimus.buildtool.status.sec")).map(_.toInt))
      .getOrElse(0)

  if (interval > 0) {
    val timer = new Timer
    val t = interval * 1000L
    timer.schedule(
      new TimerTask {
        override def run(): Unit = logStatus()
      },
      t,
      t
    )
  }

  override def startTask(scopeId: ScopeId, category: CategoryTrace, time: Instant): TaskTrace =
    new CountingTaskTrace(scopeId, category, time)

  override def startBuild(): Unit = counts.clear()

  def anomalies: Seq[(String, Boolean)] = {
    counts.asScala.flatMap { case (c, s) =>
      s.flatMap { case (id, (running, ran)) =>
        def key = s"$id:${c.categoryName}"
        val stillRunning =
          if (running > 0) Seq((s"$key still had $running tasks running at end of build", true)) else Nil
        val repeatedSingleton =
          if (c.isSingleton && ran > 1)
            Seq((s"$key ran $ran times during build (expected once only)", c.isStrictSingleton))
          else
            Nil
        stillRunning ++ repeatedSingleton
      }
    }.toSeq
  }

  override def endBuild(success: Boolean): Boolean = {
    val as = anomalies
    if (as.nonEmpty) log.error(s"CountingTrace detected anomalies in the build:\n${as.map(_._1).mkString("\n")}")
    as.collect {
      case (a, strict) if strict => a
    }.isEmpty
  }

  private val categoriesOfInterest = Seq(Signatures, Scala, Java, MemQueue)
  private def logStatus(showAll: Boolean = false): Int = {
    var totalRunning = 0
    for (
      c <- categoriesOfInterest;
      m <- Option(counts.get(c))
    ) {
      val runningIds = ArrayBuffer.empty[String]
      var completed = 0
      var restarts = 0
      m.foreach { case (scopeId, (running, started)) =>
        if (running > 0)
          runningIds += scopeId.toString
        if (started > 0 || running == 0)
          completed += 1
        if (started > 1)
          restarts += 1
        if (!c.isAsync) totalRunning += 1
      }
      log.debug(s"$c running=${runningIds.size}, completed=$completed, restarts=$restarts${runningIds
          .mkString(": ", ", ", ",")}")
    }
    totalRunning
  }
}
