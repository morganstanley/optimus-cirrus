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
package optimus.graph.diagnostics.messages

import java.lang.management.ManagementFactory
import java.text.DecimalFormat
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.core.CoreHelpers.between
import optimus.graph.OGTrace
import optimus.graph.PThreadContext
import optimus.utils.misc.Color

object OGCounter {
  val jvmStartTime: Long = ManagementFactory.getRuntimeMXBean.getStartTime
  val snappedNanoTime: Long = System.nanoTime()
  val snappedDateTime: Long = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis())

  val countFormat: DecimalFormat = new DecimalFormat("#,###")
  val counters = new Int2ObjectOpenHashMap[OGCounter[_ <: OGEvent]]
}

abstract class OGCounter[T <: OGEvent](val description: String, val collectorType: Int, template: T) {
  OGCounter.counters.put(template.counterID, this)
  def counterID: Int = template.counterID
  def defaultGraphColor: Int = template.graphColor
  final def publish(event: T): T = {
    OGTrace.observer.writeEvent(event)
    this match {
      case a: Accumulating[T @unchecked, _] => a.accumulate(event)
      case _                                =>
    }
    event
  }
}

/**
 * Trait for a value that can be accumulated, diffed, and then published as a crumbs.
 */
trait AccumulatedValue {
  def elems: Elems
  def diff(o: AccumulatedValue): AccumulatedValue // This will be mapped over a heterogeneous collection
}

/**
 * Trait for counters that can accumulate a cumulant
 */
trait Accumulating[T <: OGEvent, R <: AccumulatedValue] {
  self: OGCounter[T] =>
  // Return latest value
  def snap: R
  // Ingest and accumulate a new event
  def accumulate(event: T): Unit
}

abstract class OGCounterWithComplete[T <: OGEventWithComplete](description: String, colType: Int, template: T)
    extends OGCounter(description, colType, template) {
  def publishComplete(event: OGEventWithComplete): Unit =
    OGTrace.observer.writeEventComplete(event.counterID, event.eventID)
}

abstract class OGEvent extends Serializable {
  def eventID: Int = 0
  def counterID: Int
  var time: Long = _
  @transient // Only needed for reading multiple files
  var process: PThreadContext.Process = _
  def graphValue: Long = 1 // A default event will just default to value of 1 (a bump)
  def graphLabel(counter: OGCounter[_]): String = counter.description + " " + OGCounter.countFormat.format(graphValue)
  def graphColor: Int = new Color(0, 90, 164).getRGB // firm blue

  def this(time: Long, value: Int) = this()

  def during(start: Long, end: Long): Boolean = between(start, end, time)
}

class OGEventCompletion(val eventID: Int, val timestamp: Long)

object OGEventWithComplete {
  private val idCounter = new AtomicInteger()
}

abstract class OGEventWithComplete extends OGEvent {
  override val eventID: Int = OGEventWithComplete.idCounter.incrementAndGet()
  var duration: Long = _

  final def startTime: Long = time
  final def endTime: Long = time + duration

  def setStartAndEndTimeFromJVMStartTime(startTime: Long, endTime: Long): Unit = {
    time = TimeUnit.MILLISECONDS.toNanos(
      OGCounter.jvmStartTime + startTime) - (OGCounter.snappedDateTime - OGCounter.snappedNanoTime)
    duration = TimeUnit.MILLISECONDS.toNanos(endTime - startTime)
  }

  def setEndTime(endTime: Long): Unit = {
    if (this.duration == 0) this.duration = endTime - time
  }

  override def during(start: Long, end: Long): Boolean = between(start, end, startTime) || between(start, end, endTime)
}
