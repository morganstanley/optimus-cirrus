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

import optimus.breadcrumbs.crumbs.Properties
import optimus.graph.AwaitStackManagement
import optimus.graph.Awaitable
import optimus.graph.NodeTask
import optimus.graph.diagnostics.ap.StackAnalysis
import optimus.utils.PropertyUtils
import optimus.graph.DiagnosticSettings
import optimus.graph.OGLocalTables
import optimus.graph.PluginType
import optimus.graph.PluginType.AdaptedNodesOfAllPlugins
import optimus.graph.Scheduler
import optimus.graph.Scheduler.SchedulerSnapshot
import optimus.graph.TestableClock
import optimus.graph.diagnostics.sampling.SamplingProfiler.TopicAccumulator
import optimus.platform.util.Log

import java.util
import java.util.Objects
import scala.collection.mutable
import scala.util.control.NonFatal

private[sampling] object NodeStackSampler {
  final case class StackValue(frames: util.ArrayList[String]) {
    var weight = 0L
  }
  val maxStacks: Int = PropertyUtils.get("optimus.sampling.nodestacks.cached", 1000)

  final case class Result[R](ntsk: NodeTask, inner: NodeTask, weight: Long, value: R)

  val lostStack = StackValue(new util.ArrayList[String]())
  lostStack.frames.add("Lost")
  val lostHash = 705470547054L // meaningless unique id

  private[sampling] val pluginStateAccumulator: TopicAccumulator =
    SamplingProfiler.topicAccumulator(Properties.pluginStateTimes)
}

//noinspection ScalaUnusedSymbol // ServiceLoader
class NodeStackSampler extends StackSampler with Log {

  @volatile private var pluginSamplingContinues = true
  private var pluginSampleThread: Thread = null
  @volatile private var targetIntervalNs = 0L
  private val pluginSampleWaiter = new Object

  private val Adapted = "Adapted"
  private val Unfired = "Unfired"

  private val pluginStackRecorders = mutable.HashMap.empty[(PluginType, String), StackRecorder]
  private def getRecorder(pt: PluginType, tpe: String): StackRecorder = {
    pluginStackRecorders.getOrElseUpdate((pt, tpe), new StackRecorder(tpe + "_" + pt.name))
  }

  override def drainToIterable(sp: SamplingProfiler): Iterable[TimedStack] = { // Enqueue chains of nodes currently parked in waiting queues.
    val waitStacks = {
      val waitStackSampler = new StackRecorder("Wait")
      if (DiagnosticSettings.awaitStacks) {
        for {
          SchedulerSnapshot(_, contexts) <- Scheduler.snapshots()
          queue <- contexts.waiting
          last <- queue.lastTask()
        } {
          waitStackSampler.record(last, sp.actualPeriodMs * SamplingProfiler.NANOSPERMILLI)
        }
      }
      waitStackSampler.drainToIterable()
    }

    // Collect stacks (e.g. cache hits) from local tables. These stack recorders were necessarily created
    // without knowledge of the current SamplingProfiler, so set the target interval now.
    val localTableStacks = {
      val stackSamplers = new KnownStackRecorders
      OGLocalTables.forAllRemovables(
        _.knownStackRecorders.drainStacksTo(stackSamplers, setTargetRecordIntervalNs = targetIntervalNs))
      stackSamplers.drainToIterable()
    }

    // Collect inflight plugin stacks
    val pluginStacks = pluginStackRecorders.synchronized {
      pluginStackRecorders.values.flatMap(_.drainToIterable())
    }

    val preSplit = localTableStacks ++ waitStacks ++ pluginStacks

    preSplit

  }

  override def start(sp: SamplingProfiler): Unit = this.synchronized {
    targetIntervalNs = sp.propertyUtils.get("optimus.sampling.nodestacks.interval.ms", 100) * 1000L * 1000L
    if (Objects.isNull(pluginSampleThread)) {
      pluginSampleThread = new Thread {
        override def run(): Unit = {
          try {
            val pluginStackInterval: Long = sp.propertyUtils.get("optimus.sampling.plugin.stacks.ms", 100)
            var tSample: Long = System.currentTimeMillis()
            var nextSample: Long = tSample + pluginStackInterval
            var prevSample: Long = 0
            while (pluginSamplingContinues) {
              prevSample = tSample
              // Don't ever run more than ~10% faster than configured, even if we fell behind for some reason.
              val earliestPermissible = tSample + pluginStackInterval * 9 / 10
              while (nextSample < earliestPermissible) {
                nextSample += pluginStackInterval
              }
              tSample = System.currentTimeMillis()
              pluginSampleWaiter.synchronized {
                while (tSample < nextSample) {
                  pluginSampleWaiter.wait(nextSample - tSample)
                  tSample = System.currentTimeMillis()
                }
              }
              if (pluginSamplingContinues) {
                val adaptedNodes = new AdaptedNodesOfAllPlugins
                OGLocalTables.forAllRemovables { rt =>
                  adaptedNodes.accumulate(rt.pluginTracker.adaptedNodes)
                }
                val inFlightSample: Map[PluginType, (NodeTask, Int)] = adaptedNodes.randomUncompletedNodes()
                val unFiredSample: Map[PluginType, (NodeTask, Int)] = adaptedNodes.randomNodes(!_.pluginFired())
                val interval = tSample - prevSample
                val weightMillis = (tSample - prevSample)
                val weightNanos = weightMillis * SamplingProfiler.NANOSPERMILLI
                BaseSamplers.accumulateStats("pluginSamplingInterval", interval)
                pluginStackRecorders.synchronized {
                  inFlightSample.foreach { case (pt, (ntsk, n)) =>
                    if (n > 0) {
                      val recorder = getRecorder(pt, Adapted)
                      recorder.record(ntsk, weightNanos)
                      NodeStackSampler.pluginStateAccumulator.accumulate(Adapted, pt.name, weightMillis)
                    }
                  }
                  unFiredSample.foreach { case (pt, (ntsk, n)) =>
                    if (n > 0) {
                      val recorder = getRecorder(pt, Unfired)
                      recorder.record(ntsk, weightNanos)
                      NodeStackSampler.pluginStateAccumulator.accumulate(Unfired, pt.name, weightMillis)
                    }
                  }
                }
              }
            }
            log.info(s"Exiting $this thread")
          } catch {
            case NonFatal(t) =>
              log.error("Plugin sampling thread failed", t)
          }
        }
      }
    }

    pluginSampleThread.setName("PluginSampleThread")
    log.info(s"Starting $pluginSampleThread")
    pluginSampleThread.setDaemon(true)
    pluginSampleThread.start()
  }

  override def shutdown(): Unit = {
    pluginSampleWaiter.synchronized {
      log.info(s"Shutting down $pluginSampleThread")
      targetIntervalNs = 0L
      pluginSamplingContinues = false
      pluginSampleWaiter.notifyAll()
    }
  }

}

object KnownStackRecorders extends Enumeration {
  val CacheHit = Value
  val ids: Array[Value] = (0 until maxId).map(apply).toArray
}

class KnownStackRecorders {
  private val recorders =
    KnownStackRecorders.ids.map(id => new StackRecorder(id.toString))
  def drainStacksTo(srs: KnownStackRecorders, setTargetRecordIntervalNs: Long): Unit = {
    (0 until KnownStackRecorders.maxId).foreach(id =>
      recorders(id).drainTo(srs.recorders(id), false, setTargetRecordIntervalNs))
  }

  def expungeTo(srs: KnownStackRecorders): Unit = {
    (0 until KnownStackRecorders.maxId).foreach(id => recorders(id).drainTo(srs.recorders(id), true, -1L))
  }

  private[sampling] def drainToIterable(): Iterable[TimedStack] = recorders.flatMap(_.drainToIterable())

  def apply(id: KnownStackRecorders.Value): StackRecorder = recorders(id.id)
  def apply(id: Int): StackRecorder = recorders(id)
}

class StackRecorder(eventName: String) {
  import NodeStackSampler._
  private var skipBeforeSample = 1L
  private var iSkip = 0L
  @volatile private var accruedWeight = 0L
  private val eventFrame = StackAnalysis.customEventFrame(eventName)
  private val recordedStacks = mutable.HashMap.empty[Long, StackValue]
  private var recordingTime = TestableClock.nanoTime()
  @volatile private var targetIntervalNs = 0L

  private[sampling] def drainToIterable(): Iterable[TimedStack] = {
    val svs = this.synchronized {
      val svs = recordedStacks.values.toArray
      recordedStacks.clear()
      svs
    }
    val ret = svs.map { sv =>
      val innerFirst = sv.frames
      val n = innerFirst.size
      val outerFirst = new Array[String](n + 1)
      outerFirst(0) = eventFrame
      for (i <- 1 to n)
        outerFirst(i) = innerFirst.get(n - i)
      TimedStack(outerFirst, sv.weight)
    }
    recordedStacks.clear()
    ret
  }

  private[sampling] def drainTo(sr: StackRecorder, expunge: Boolean, setTargetRecordIntervalNs: Long): Unit =
    this.synchronized {
      if (setTargetRecordIntervalNs >= 0L)
        targetIntervalNs = setTargetRecordIntervalNs
      recordedStacks.foreach { case (h, sv) =>
        val w = sv.weight
        sv.weight = 0
        sr.recordedStacks.getOrElseUpdate(h, sv).weight += w
      }
      if (expunge) {
        // If we're going away, we'll never sample again, so all our accrued weight is lost
        sr.recordedStacks.getOrElseUpdate(lostHash, lostStack).weight += accruedWeight
        accruedWeight = 0 // unnecessary, but for emphasis!
      }
      recordedStacks.clear()
    }

  private def addWeightToStack(h: Long, stack: util.ArrayList[String], weight: Long): Unit = this.synchronized {
    // Make sure the stack exists
    val sv = recordedStacks.getOrElseUpdate(h, StackValue(stack))
    sv.weight += weight
  }

  def record(ntsk: Awaitable, weight: Long): Unit = {
    val h = ntsk.getLauncherStackHash
    val stack = AwaitStackManagement.awaitStack(ntsk)
    addWeightToStack(h, stack, weight)
  }

  private def recordAndAdjustTiming(ntsk: NodeTask, extraMods: String): Unit = {
    val t = TestableClock.nanoTime()
    if (skipBeforeSample == 0)
      skipBeforeSample = 1
    else {
      val dt = t - recordingTime
      // If we're within a factor of 2 of target interval, try to adjust by ratio.
      // If we're way off, feel our way back by factors of 2.
      if (dt > targetIntervalNs / 2 && dt < targetIntervalNs * 2)
        skipBeforeSample = (skipBeforeSample * targetIntervalNs) / dt
      else if (dt > targetIntervalNs) skipBeforeSample /= 2
      else skipBeforeSample *= 2
    }
    iSkip = skipBeforeSample
    recordingTime = t
    if (accruedWeight != 0) {
      val h = ntsk.getLauncherStackHash
      if (recordedStacks.size < maxStacks && h != 0) {
        val stack = AwaitStackManagement.awaitStack(ntsk, extraMods)
        addWeightToStack(h, stack, accruedWeight)
        accruedWeight = 0
      }
    }
  }

  def accrueAndMaybeRecord(ntsk: NodeTask, weight: Long, extraMods: String): Unit = if (targetIntervalNs > 0L) {
    iSkip -= 1
    accruedWeight += weight
    if (iSkip <= 0)
      recordAndAdjustTiming(ntsk, extraMods)
  }

}
