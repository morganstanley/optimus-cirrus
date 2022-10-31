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
package optimus.logging

import java.text.DateFormatSymbols
import java.text.FieldPosition
import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone

import ch.qos.logback.classic.pattern.ClassicConverter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.CoreConstants

class DateConverter extends ClassicConverter {
  private[this] var formatter: DateFormatter = null
  override def start(): Unit = {
    import scala.jdk.CollectionConverters._
    val options: List[String] = getOptionList match {
      case null => List()
      case list => list.asScala.toList
    }
    val datePattern: String = options.headOption match {
      case None                            => CoreConstants.ISO8601_PATTERN
      case Some(CoreConstants.ISO8601_STR) => CoreConstants.ISO8601_PATTERN
      case Some(specified)                 => specified
    }

    formatter =
      try new DateFormatter(datePattern)
      catch {
        case e: IllegalArgumentException =>
          addWarn(s"Cannot create DateFormatter - probably the pattern isn't legal '$datePattern'", e)
          new DateFormatter(CoreConstants.ISO8601_PATTERN)
      }
    options match {
      case _ :: timezone :: _ =>
        formatter.setTimeZone(TimeZone.getTimeZone(timezone))
      case _ =>
    }
  }
  override def convert(event: ILoggingEvent): String = formatter.format(event.getTimeStamp)
}
object DateFormatter {
  // we try to use java.text.DontCareFieldPosition.INSTANCE, because it make the generation of dates faster
  // if we cant access then we fall back to  new FieldPosition(0), which is functionally the same, but slower and allocates a little
  private val dontCareFieldPosition = buildField

  private[this] def buildField = {
    try {
      val instance = Class.forName("java.text.DontCareFieldPosition").getDeclaredField("INSTANCE")
      instance.setAccessible(true)
      instance.get(null).asInstanceOf[FieldPosition]
    } catch {
      case e: ReflectiveOperationException =>
        // this will cause additional allocation - e.g. via getFieldDelegate and some extra CPU as there are explicit
        // check  for DontCareFieldPosition.INSTANCE in the code. It has no functional effect
        System.err.println(
          "unable to access java.text.DontCareFieldPosition.INSTANCE - some optimisations disabled - " + e.getMessage)
        new FieldPosition(0)
    }
  }

}
class DateFormatter(val pattern: String) {
  // only called with a synchronized lock on this
  private val dateFormat = new SimpleDateFormat(pattern, DateFormatSymbols.getInstance)
  // cached values, only mutated or read with a lock on this
  private var prevTime = 0L
  private var prevFormatted = ""
  // avoid allocation in each format operation
  private val scratchDate = new Date
  private val scratchStringBuffer = new StringBuffer

  final def format(now: Long): String = synchronized {
    // SimpleDateFormat is not thread-safe.
    if (now != prevTime) {
      prevTime = now
      scratchDate.setTime(now)
      scratchStringBuffer.setLength(0)
      dateFormat.format(scratchDate, scratchStringBuffer, DateFormatter.dontCareFieldPosition)
      prevFormatted = scratchStringBuffer.toString
    }
    prevFormatted

  }
  def setTimeZone(tz: TimeZone): Unit = synchronized { dateFormat.setTimeZone(tz) }
}
