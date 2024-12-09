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

import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.Properties.profDALBatches
import optimus.breadcrumbs.crumbs.Properties.profDALCommands

import java.lang.management.ManagementFactory
import optimus.graph.OGTraceCounter
import optimus.graph.OGTraceCounter._
import optimus.graph.tracking.EventCause
import optimus.ui.ScenarioReference
import optimus.utils.MacroUtils.SourceLocation
import optimus.utils.misc.Color

import java.util.concurrent.atomic.AtomicReference

object AllOGCounters {

  // register in optimus.graph.OGTraceReaderEventProcessor.counters
  val allCounters: Array[OGCounter[_]] =
    Array(
      BookmarkCounter,
      AdaptedCounter,
      DALNodesInFlightCounter,
      DALRequestsInFlightCounter,
      DALRequestCounter,
      NativeMemoryCounter,
      HeapMemoryCounter,
      GCCounter,
      CacheEntriesCounter,
      CacheClearCounter,
      SyncStackCounter,
      BlockingWaitCounter,
      UserActionCounter,
      ActiveGUICounter,
      StartupEventCounter,
      HandlerStepEventCounter
    )
}

final case class BookmarkEvent(message: String = "", color: Int = 0) extends OGEvent {
  override def counterID: Int = CounterId.BOOKMARKS_ID.id
  override def graphColor: Int = color
  override def graphLabel(counter: OGCounter[_]): String = counter.description + " " + message
}

object BookmarkCounter extends OGCounter("Bookmarks", BOOKMARK, BookmarkEvent()) {
  def report(msg: String, color: Int = 0): Unit = publish(BookmarkEvent(msg, color))
}

final case class AdaptedEvent(count: Int = 0) extends OGEvent {
  override def counterID: Int = CounterId.ADAPTED_NODES_ID.id
  override def graphColor: Int = new Color(232, 0, 0, 150).getRGB // red
  override def graphValue: Long = count
}

object AdaptedCounter extends OGCounter("Adapted Nodes", DISCRETE_VALUE, AdaptedEvent()) {
  def report(count: Int): Unit = publish(AdaptedEvent(count))
}

/** DAL plugin intercepted nodes */
final case class DALNodesInFlightEvent(count: Int = 0) extends OGEvent {
  override def counterID: Int = CounterId.DAL_NODES_ID.id
  override def graphValue: Long = count
}

object DALNodesInFlightCounter extends OGCounter("DAL Nodes", DISCRETE_VALUE, DALNodesInFlightEvent()) {
  def report(count: Int): Unit = publish(DALNodesInFlightEvent(count))
}

final case class DALRequestsInFlightEvent(reqID: String = "", count: Int = 0) extends OGEvent {
  override def counterID: Int = CounterId.DAL_REQS_FLIGHT_ID.id
  override def graphColor: Int = new Color(0, 0, 255, 150).getRGB // blue
  override def graphValue: Long = count
}

object DALRequestsInFlightCounter extends OGCounter("DAL Reqs. in Flight", DISCRETE_VALUE, DALRequestsInFlightEvent()) {
  def report(reqID: String, count: Int): Unit = publish(DALRequestsInFlightEvent(reqID, count))
}

// NB: This is distinct from dalNodesInFlight since several queries may be batched into one request by the async client
final case class DALRequestEvent(
    reqID: String = "",
    env: String = "",
    batchSize: Int = 0,
    nodeIDs: Array[Int] = null,
    locations: String = "",
    other: String = "")
    extends OGEventWithComplete {
  override def counterID: Int = CounterId.DAL_REQS_ID.id
  override def graphColor: Int = new Color(0, 0, 255, 150).getRGB // blue
  override def graphValue: Long = batchSize
}

final case class DalRequestCumulant(batches: Int, commands: Int) extends AccumulatedValue {
  override def elems: Elems = Elems(profDALBatches -> batches, profDALCommands -> commands)
  override def diff(o: AccumulatedValue) = o match {
    case DalRequestCumulant(b, c) => DalRequestCumulant(batches - b, commands - c)
    case _                        => this
  }
}
object DALRequestCounter
    extends OGCounterWithComplete("DAL Reqs.", DISCRETE_VALUE, DALRequestEvent())
    with Accumulating[DALRequestEvent, DalRequestCumulant] {
  private val c = new AtomicReference(DalRequestCumulant(0, 0))
  def report(
      requestUuid: String,
      dalEnv: String,
      size: Int,
      nodeIDs: Array[Int],
      locations: String,
      otherStuff: String): DALRequestEvent = {
    publish(DALRequestEvent(requestUuid, dalEnv, size, nodeIDs, locations, otherStuff))
  }
  override def accumulate(event: DALRequestEvent): Unit = c.updateAndGet { case DalRequestCumulant(batches, commands) =>
    DalRequestCumulant(batches + 1, commands + event.batchSize)
  }
  override def snap: DalRequestCumulant = c.get()
}

final case class NativeMemoryEvent(sizeMB: Long = 0) extends OGEvent {
  override def counterID: Int = CounterId.NATIVE_MEMORY_ID.id
  override def graphColor: Int = new Color(126, 27, 195, 230).getRGB // purple
}

object NativeMemoryCounter extends OGCounter("Native Memory", SAMPLED_VALUE, NativeMemoryEvent()) {
  def report(size: Long): Unit = if (size > 0) publish(NativeMemoryEvent(size))
}

final case class HeapMemoryEvent(size: Long = 0) extends OGEvent {
  override def counterID: Int = CounterId.HEAP_MEMORY_ID.id
  override def graphValue: Long = size / (1024 * 1024)
  override def graphLabel(counter: OGCounter[_]): String = super.graphLabel(counter) + " MB"
  override def graphColor: Int = new Color(229, 108, 27, 230).getRGB // orange
}

object HeapMemoryCounter extends OGCounter("Heap Memory", SAMPLED_VALUE, HeapMemoryEvent()) {
  private val memoryBean = ManagementFactory.getMemoryMXBean

  def report(size: Long): Unit = publish(HeapMemoryEvent(size))
  def reportCurrent(): Unit = report(memoryBean.getHeapMemoryUsage.getUsed)
}

object GCEvent {
  val other = 0
  val minor = 1
  val major = 2
}

class GCEvent(
    val gcType: Int = 0,
    val gcCause: String = "",
    startTime: Long = 0L,
    endTime: Long = 0L,
    val memoryCleared: Long = 0L)
    extends OGEventWithComplete {
  setStartAndEndTimeFromJVMStartTime(startTime, endTime)

  def gcTypeDescription: String = {
    if (gcType == GCEvent.minor) "minor" else if (gcType == GCEvent.major) "major" else "other"
  }
  override def counterID: Int = CounterId.GC_ID.id
  override def graphValue: Long = memoryCleared / (1024 * 1024)
  override def graphLabel(counter: OGCounter[_]): String = super.graphLabel(counter) + " MB"
  override def graphColor: Int = new Color(235, 125, 205, 230).getRGB // pink
}

object GCCounter extends OGCounter("GC", BLOCK_VALUE, new GCEvent()) {
  def report(eventType: Int, gcCause: String, startTime: Long, endTime: Long, memoryCleared: Long): Unit = {
    publish(new GCEvent(eventType, gcCause, startTime, endTime, memoryCleared))
  }
}

final case class CacheEntriesEvent(totalNumRemaining: Long = 0) extends OGEvent {
  override def counterID: Int = CounterId.CACHE_ENTRIES_ID.id
  override def graphColor: Int = new Color(29, 170, 174, 230).getRGB // turquoise
}

object CacheEntriesCounter extends OGCounter("Cache Entries", DISCRETE_VALUE, CacheEntriesEvent()) {
  def report(count: Long): Unit = publish(CacheEntriesEvent(count))
}

final case class CacheClearEvent(
    description: String = "",
    name: String = "",
    numRemoved: Long = 0,
    numRemaining: Long = 0,
    totalNumRemaining: Long = 0)
    extends OGEvent {
  override def counterID: Int = CounterId.CACHE_CLEARS_ID.id
  override def graphColor: Int = new Color(21, 144, 73, 210).getRGB // green
  override def graphValue: Long = numRemoved
}

object CacheClearCounter extends OGCounter("Cache Clears", DISCRETE_VALUE, CacheClearEvent())

final case class SyncStackEvent(
    description: String = "",
    ssid: Int = 0,
    task: String = "",
    pluginType: String = "",
    count: Int = 0,
    isCritical: Boolean = false)
    extends OGEvent {
  override def counterID: Int = CounterId.SYNC_STACKS_ID.id
  override def graphColor: Int = Color.red.getRGB
}

object SyncStackCounter extends OGCounter("Sync Stacks", BOOKMARK, SyncStackEvent())

final case class UserActionEvent(description: String = "") extends OGEvent {
  override def counterID: Int = CounterId.USER_ACTIONS_ID.id
  override def graphColor: Int = new Color(126, 27, 195, 230).getRGB // purple
}

object UserActionCounter extends OGCounter("User Actions", DISCRETE_VALUE, UserActionEvent())

final case class ActiveGUIEvent(
    eventType: OGTraceCounter.UIState = UIState.Idle,
    activePeriodSec: Double = 0,
    timeSinceLastChangeSec: Double = 0)
    extends OGEvent {
  override def counterID: Int = CounterId.ACTIVE_GUI_ID.id
}

object ActiveGUICounter extends OGCounter("GUI Active/Idle State", DISCRETE_VALUE, ActiveGUIEvent()) {
  def reportActive(activePeriodSec: Double, timeSinceLastChangeSec: Double): Unit = {
    publish(ActiveGUIEvent(OGTraceCounter.UIState.Awake, activePeriodSec, timeSinceLastChangeSec))
  }

  def reportIdle(activePeriodSec: Double, timeSinceLastChangeSec: Double): Unit = {
    publish(ActiveGUIEvent(OGTraceCounter.UIState.Idle, activePeriodSec, timeSinceLastChangeSec))
  }
}

/** Event with optional descriptor and source location (so that we can print source on events) */
final case class EventDescription(description: String, sourceLocation: String)
object EventDescription {
  val empty: EventDescription = EventDescription("", "")
  def apply(description: String): EventDescription = EventDescription(description, "")
  def apply(description: String, sourceLocation: SourceLocation): EventDescription = {
    val location = sourceLocation.clickableSource
    val suffix = if (description.nonEmpty) " [" + description + "]" else ""
    EventDescription(sourceLocation.method + suffix, location)
  }
  def apply(sourceLocation: SourceLocation): EventDescription = apply(sourceLocation.method, sourceLocation)
}

sealed abstract class TimedEvent extends OGEventWithComplete {
  def event: EventDescription
  final override def graphValue: Long = duration
  final override def graphLabel(counter: OGCounter[_]): String = event.description
}

final case class HandlerStepEvent(scenarioRef: String, cause: String, event: EventDescription) extends TimedEvent {
  override def counterID: Int = CounterId.HANDLER_ID.id
  override def graphColor: Int = Color.MAGENTA.getRGB
}

object HandlerStepEventCounter
    extends OGCounterWithComplete("Handler Step Events", BOOKMARK, HandlerStepEvent("", "", EventDescription.empty)) {
  def report(scenario: ScenarioReference, cause: EventCause, description: EventDescription): HandlerStepEvent = {
    val event = HandlerStepEvent(scenario.toString, cause.root.cause, description)
    publish(event)
    event
  }

  def reportCompleted(event: HandlerStepEvent): Unit = publishComplete(event)
}

final case class DTQEvent(scenarioRef: String, isUpdate: Boolean, event: EventDescription) extends TimedEvent {
  override def counterID: Int = CounterId.DTQ_ID.id
  override def graphColor: Int = Color.ORANGE.getRGB
}

object DTQEventCounter
    extends OGCounterWithComplete(
      "Dependency Tracker Queue Events",
      BOOKMARK,
      DTQEvent("", isUpdate = false, EventDescription.empty)) {

  def report(scenario: ScenarioReference, cause: EventCause, isUpdate: Boolean): DTQEvent = {
    val event = DTQEvent(scenario.toString, isUpdate, EventDescription(cause.root.cause))
    publish(event)
    event
  }

  def reportCompleted(event: DTQEvent): Unit = publishComplete(event)
}

sealed abstract class TimedEventCounter[T <: TimedEvent](description: String, event: T)
    extends OGCounterWithComplete(description, BOOKMARK, event) {

  protected def create(description: EventDescription): T

  def report(description: EventDescription): T = {
    val e = create(description)
    publish(e)
    e
  }

  def reportCompleted(event: TimedEvent): Unit = publishComplete(event)
}

final case class StartupEvent(event: EventDescription = EventDescription.empty) extends TimedEvent {
  override def counterID: Int = CounterId.STARTUP_ID.id
  override def graphColor: Int = Color.CYAN.getRGB
}

object StartupEventCounter extends TimedEventCounter("Startup Events", StartupEvent()) {
  override def create(description: EventDescription): StartupEvent = StartupEvent(description)

  def report(description: String)(implicit sourceLocation: SourceLocation): StartupEvent =
    report(EventDescription(description, sourceLocation))

  def reporting[T](description: String)(f: => T)(implicit sourceLocation: SourceLocation): T = {
    val eventDescription = EventDescription(description, sourceLocation)
    val event = report(eventDescription)
    val result = f
    reportCompleted(event)
    result
  }
}

object BlockingWaitEvent {
  def apply(): BlockingWaitEvent =
    new BlockingWaitEvent(
      threadName = "",
      event = EventDescription.empty,
      jstack = null,
      awaitedTaskID = 0,
      endOfChainTaskID = 0)
}

final case class BlockingWaitEvent(
    threadName: String,
    event: EventDescription,
    jstack: Exception,
    awaitedTaskID: Int,
    endOfChainTaskID: Int)
    extends TimedEvent {
  override def counterID: Int = CounterId.BLOCKING_ID.id
}

object BlockingWaitCounter extends TimedEventCounter("Blocking Waits", BlockingWaitEvent()) {
  def report(
      threadName: String,
      awaitedTask: String,
      jstack: Exception,
      awaitedTaskID: Int,
      endOfChainTaskID: Int): BlockingWaitEvent = {
    val e = BlockingWaitEvent(threadName, EventDescription(awaitedTask), jstack, awaitedTaskID, endOfChainTaskID)
    publish(e)
    e
  }

  // not really used, just needs an override
  override protected def create(description: EventDescription): BlockingWaitEvent = BlockingWaitEvent()
}
