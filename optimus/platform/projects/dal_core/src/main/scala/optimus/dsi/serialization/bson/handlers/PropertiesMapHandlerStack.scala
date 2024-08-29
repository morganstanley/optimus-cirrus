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
package optimus.dsi.serialization.bson.handlers

import java.time._

import optimus.dsi.serialization.bson.BSONCallback
import optimus.utils.datetime._
import optimus.platform.dsi.bitemporal.DateTimeSerialization

import scala.reflect.ClassTag

/**
 * Represents a Stack of handlers to parse BSON representation of a map of properties (Map[String, Any] where Any
 * belongs to a small set of types).
 *
 * For optimisation it holds a simple pool of most frequently used handlers and element parsers.
 *
 * This class uses thread local variables so is tied to use in a single thread.
 */
private[optimus] class PropertiesMapHandlerStack {
  import PropertiesMapHandlerStack._
  lazy val byteArrayParser = new UnaryElemParser(markIdent)
  lazy val localDateElemParser = new UnaryElemParser(LocalDateOps.ofModifiedJulianDay)
  lazy val localTimeElemParser = new UnaryElemParser(LocalTime.ofNanoOfDay)
  lazy val offsetTimeElemParser = new LongStringElemParser(offsetTime)
  lazy val zonedDateTimeElemParser = new LongStringElemParser(toZonedDateTime)
  lazy val zoneIdElemParser = new UnaryElemParser(toZoneId)
  lazy val periodElemParser = new PeriodElemParser()
  lazy val buffElemParser = new BuffElemParser()

  protected val seqHandlerPool = seqHandlerPoolThreadLocal.get()
  protected val propertiesHandlerPool = propertiesHandlerPoolThreadLocal.get()

  protected var stack = stackThreadLocal.get()
  // pointing at the next place to insert
  protected var stackPos = 0

  def elems: List[BSONCallback] = {
    val retArray = new Array[BSONCallback](stackPos)
    Array.copy(stack, 0, retArray, 0, stackPos)
    retArray.toList
  }

  def head: BSONCallback = stack(stackPos - 1)

  def pop(): BSONCallback = {
    stackPos -= 1
    val popped = stack(stackPos)
    popped match {
      case h: PropertiesMapSeqHandler =>
        seqHandlerPool.put(h)
      case h: PropertiesHandler =>
        propertiesHandlerPool.put(h)
      case _ => ()
    }
    popped
  }

  def newSeqHandler(parentName: String, parent: PropertyHolder): PropertiesMapSeqHandler = {
    if (seqHandlerPool.hasElem) {
      val ah = seqHandlerPool.get()
      ah.reinit(parentName, parent, this)
      ah
    } else {
      new PropertiesMapSeqHandler(parentName, parent, this)
    }
  }

  def newPropertiesHandler(parentName: String, parent: PropertyHolder): PropertiesHandler = {
    val phToUse = if (propertiesHandlerPool.hasElem) {
      val ph = propertiesHandlerPool.get()
      ph.reinit(parentName, parent, this)
      ph
    } else {
      new PropertiesHandler(parentName, parent, this)
    }
    phToUse
  }

  def push(elem: BSONCallback): PropertiesMapHandlerStack = {
    if (stackPos >= stack.length) {
      val newStack = new Array[BSONCallback](stack.length * 2 + 1)
      Array.copy(stack, 0, newStack, 0, stack.length)
      stackThreadLocal.set(newStack)
      stack = newStack
    }
    stack(stackPos) = elem
    stackPos += 1
    this
  }

  def markIdent(bytes: Array[Byte]): Array[Byte] = bytes

  def offsetTime(longVal: Long, stringVal: String): OffsetTime =
    OffsetTime.of(LocalTime.ofNanoOfDay(longVal), ZoneOffset.of(stringVal))

  def toZonedDateTime(longVal: Long, stringVal: String): ZonedDateTime = {
    val local = DateTimeSerialization.toInstant(longVal)
    val zone = toZoneId(stringVal)
    ZonedDateTime.ofInstant(local, zone)
  }

  def toZoneId(stringVal: String): ZoneId = ZoneId.of(stringVal)
}

class HandlerPool[T](poolType: Class[T], size: Int, maxSize: Int)(implicit m: ClassTag[T]) {
  var pool = new Array[T](size)
  var pos = 0

  def hasElem: Boolean = pos > 0

  def get(): T = {
    val ah = pool(pos - 1)
    pos -= 1
    ah
  }

  def put(elem: T): Unit = {
    if (pos < maxSize) {
      if (pos >= pool.length) {
        val newSize = Math.min(maxSize, pool.size * 3 / 2 + 1)
        val newPool = new Array[T](newSize)
        System.arraycopy(pool, 0, newPool, 0, pos)
        pool = newPool
      }
      pool(pos) = elem
      pos += 1
    }
  }
}

object PropertiesMapHandlerStack {
  val seqHandlerPoolThreadLocal = new ThreadLocal[HandlerPool[PropertiesMapSeqHandler]] {
    override def initialValue(): HandlerPool[PropertiesMapSeqHandler] =
      new HandlerPool(classOf[PropertiesMapSeqHandler], size = 5, maxSize = 15)
  }

  val propertiesHandlerPoolThreadLocal = new ThreadLocal[HandlerPool[PropertiesHandler]] {
    override def initialValue(): HandlerPool[PropertiesHandler] =
      new HandlerPool(classOf[PropertiesHandler], size = 5, maxSize = 15)
  }

  val stackThreadLocal = new ThreadLocal[Array[BSONCallback]] {
    override def initialValue(): Array[BSONCallback] = new Array[BSONCallback](5)
  }
}
