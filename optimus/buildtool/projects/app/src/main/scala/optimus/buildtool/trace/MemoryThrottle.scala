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

import java.lang.management.ManagementFactory
import java.lang.management.MemoryType
import java.lang.management.MemoryUsage
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.config.ScopeId
import optimus.platform._

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq

object MemoryThrottle {
  private val log = getLogger(this)

  private def memoryBeans =
    ManagementFactory.getMemoryPoolMXBeans.asScala.toIndexedSeq.filter(_.getType == MemoryType.HEAP)
  private def getUsages: Seq[MemoryUsage] = memoryBeans.map(_.getUsage)
  abstract private class Mem(freeMb: Long) {
    def >=(l: Long) = freeMb >= l
    def <(l: Long) = freeMb < l
    def toString: String
  }
  private def getFree = {
    val usages = getUsages
    val max = usages.map(_.getMax).sum / (1024L * 1024L) // MB
    val used = usages.map(_.getUsed).sum / (1024L * 1024L) // MB
    val freeMemMb: Long = max - used
    new Mem(freeMemMb) {
      override def toString = s"free=$freeMemMb max=$max used=$used: ${usages.mkString(";")}"
    }
  }

  /**
   * parses supplied string (or defaults to sytem property) in the format
   * minimumFreeMemoryInGBs,delayBetweenPollsInSecs,maxNumberOfDelaysBeforeFailure
   */
  def fromConfigStringOrSysProp(memConfig: Option[String]): Option[MemoryThrottle] = {
    memConfig
      .orElse(Option(System.getProperty("optimus.buildtool.memconfig")))
      .flatMap { config =>
        try {
          val nums = config.split(',').map(_.toInt)
          log.debug(s"Configuring memory: ${nums.mkString(",")}")
          val Array(freeMemGbDelay, freeMemGbGC, delaySec, maxDelays) = nums
          val minFreeMbDelay = 1024L * freeMemGbDelay
          val minFreeMbGC = 1024L * freeMemGbGC
          val memDelayMillis = 1000L * delaySec
          Some(
            new MemoryThrottle(
              minFreeMbDelay = minFreeMbDelay,
              minFreeMbGC - minFreeMbGC,
              memDelayMillis = memDelayMillis,
              maxDelays = maxDelays))
        } catch {
          case t: Throwable =>
            log.error(s"Can't config MemoryThrottle", t)
            None
        }
      }
  }
}

class MemoryThrottle(minFreeMbDelay: Long, minFreeMbGC: Long, memDelayMillis: Long, maxDelays: Int)
    extends DefaultObtTraceListener {
  import MemoryThrottle._

  private val running: AtomicInteger = new AtomicInteger(0)
  private val numDelays: AtomicInteger = new AtomicInteger(0)
  private val numGCs: AtomicInteger = new AtomicInteger(0)

  @async override def throttleIfLowMem$NF[T](id: ScopeId)(fn: NodeFunction0[T]): T = {
    var free = getFree
    def info(more: String) = s"[$id] $more $free"

    if (free >= minFreeMbDelay && free >= minFreeMbGC) {
      log.debug(info("continuing"))
    } else if (free >= minFreeMbDelay && free < minFreeMbGC) {
      val n = numGCs.incrementAndGet()
      log.info(info(s"< $minFreeMbGC, forcing GC ($n)"))
      System.gc()
    }
    // freeMemMb < minFreeMbDelay: try delaying task startup
    else {
      val trace = ObtTrace.startTask(id, MemQueue)
      var nDelay = 0
      var continue = true
      while (continue) {
        log.warn(s"$free < $minFreeMbDelay Mb, triggering gc")
        System.gc()
        numGCs.incrementAndGet()
        if (running.get() == 0) {
          trace.end(true)
          continue = false
          log.warn(info(s"No tasks running, might as well resume after $nDelay delays"))
        } else if (nDelay > maxDelays) {
          val msg = info(s"$nDelay > $maxDelays: too many delays!")
          trace.publishMessages(Seq(CompilationMessage.error(msg)))
          trace.end(false)
          continue = false
          throw new TimeoutException(msg)
        } else {
          nDelay += 1
          log.warn(info(s"Delaying $memDelayMillis (n=$nDelay)"))
          numDelays.incrementAndGet()
          delay(memDelayMillis)
          free = getFree
          if (free >= minFreeMbDelay) {
            continue = false
            trace.end(true)
            log.warn(info(s"$free >= $minFreeMbDelay, resuming after $nDelay delays"))
          }
        }
      }
    }
    // Use aseq to avoid worries about reordering. Note that there is a small race here between checking running above
    // and updating it here, but we're not too concerned because this is a rough heuristic anyway
    val (_, result, _) = aseq(
      running.incrementAndGet(),
      asyncResult(EvaluationContext.cancelScope)(fn()),
      running.decrementAndGet()
    )
    result.value
  }

  override def endBuild(success: Boolean): Boolean = {
    val nd = numDelays.get()
    val ngc = numGCs.get()
    if (nd > 0 || ngc > 0)
      log.warn(s"$nd delay(s) due to memory pressure, $ngc forced garbage collection(s)")
    else
      log.debug(s"$nd delay(s) due to memory pressure, $ngc forced garbage collection(s)")
    true
  }
}
