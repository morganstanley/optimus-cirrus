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
package optimus.graph.diagnostics

import java.util.Comparator
import java.util.{ArrayList => JArrayList}

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import optimus.graph.PThreadContext
import optimus.graph.diagnostics.messages.OGCounter
import optimus.graph.diagnostics.messages.OGEvent
import optimus.graph.diagnostics.messages.OGEventCompletion
import optimus.graph.diagnostics.messages.OGEventWithComplete

import scala.collection.mutable.ArrayBuffer

/** OGTraceCounter plus UI support */
class OGCounterView[T <: OGEvent](val id: Int, val ogtCounter: OGCounter[T], val ctx: PThreadContext.Process) {
  def this(id: Int, ogtCounter: OGCounter[T]) = this(id, ogtCounter, PThreadContext.Process.none)
  def this(counter: OGCounter[T]) = this(counter.counterID, counter)

  val events: JArrayList[T] = new JArrayList[T]
  private val completions: Int2ObjectOpenHashMap[OGEventCompletion] = new Int2ObjectOpenHashMap[OGEventCompletion]()
  var minValue: Long = Long.MaxValue
  var maxValue: Long = Long.MinValue

  final def collectorType: Int = ogtCounter.collectorType
  final def description: String = ogtCounter.description
  override def toString: String = ogtCounter.description

  def addCompletion(eventID: Int, timestamp: Long): Unit =
    completions.put(eventID, new OGEventCompletion(eventID, timestamp))

  /**
   * Returns sorted and Values only (no ValueChange) view
   */
  final def asViewable: OGCounterView[T] = {
    val counter = new OGCounterView[T](id, ogtCounter)
    counter.events.addAll(events)
    counter.events.sort(Comparator.comparingLong(_.time))

    var i = 0
    var lastValue: OGEvent = null
    while (i < counter.events.size()) {
      val event = counter.events.get(i)
      if (event.graphValue > counter.maxValue) counter.maxValue = event.graphValue
      if (event.graphValue < counter.minValue) counter.minValue = event.graphValue

      val eventCompletion = completions.get(event.eventID)
      if (eventCompletion ne null) {
        event.asInstanceOf[OGEventWithComplete].setEndTime(eventCompletion.timestamp)
        completions.remove(event.eventID)
      }

      lastValue = event
      i += 1
    }
    counter
  }

  final def eventsBetween(start: Long, end: Long): ArrayBuffer[T] = {
    val r = ArrayBuffer[T]()
    var i = 0
    while (i < events.size()) {
      val e = events.get(i)
      if (e.during(start, end))
        r += e
      i += 1
    }
    r
  }

  /* Find the first item in the list of event that matches condition */
  final def findFirst(matcher: T => Boolean): T = {
    var r = null.asInstanceOf[T]
    var i = 0
    while (i < events.size()) {
      val e = events.get(i)
      if (matcher(e)) {
        r = e
        i = events.size()
      }
      i += 1
    }
    r
  }
}
