/* /*
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

import java.lang.StringBuilder

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.pattern.InternalPatternLayout

/** A version of PatternLayout that reuses a single buffer per thread. */
final class FastPatternLayout extends AbstractFastPatternLayout {
  private[this] val _buffer = ThreadLocal.withInitial[StringBuilder](() => new StringBuilder(512))
  override protected[this] def buffer: StringBuilder = _buffer.get
  override protected[this] def dropBuffer(): Unit = { _buffer.remove() }
}

/** A version of PatternLayout that reuses a single buffer. Not thread-safe. */
final class FastPatternLayoutST extends AbstractFastPatternLayout {
  protected[this] val buffer = new StringBuilder(512)
  override protected[this] def dropBuffer(): Unit = { buffer.setLength(512); buffer.trimToSize() }
}

sealed abstract class AbstractFastPatternLayout extends InternalPatternLayout {
  protected[this] def buffer: StringBuilder
  protected[this] def dropBuffer(): Unit

  override final def writeLoopOnConverters(event: ILoggingEvent): String = {
    val buffer = this.buffer
    buffer.setLength(0)
    writeLayoutTo(buffer, event)
    val result = buffer.toString
    // if we get a long line then lets not hold on to it
    if (result.length > 10000000 /* 10 M */ )
      this.dropBuffer()
    result
  }
  final def writeLayoutTo(buffer: StringBuilder, event: ILoggingEvent): Unit = {
    if (isStarted) {
      var converter = getHead
      while (converter ne null) {
        converter.write(buffer, event)
        converter = converter.getNext
      }
    }
  }

  // extensions
  final def doLayout(event: ILoggingEvent, buffer: EncoderBuffer): Unit = {
    if (isStarted) {
      var converter = getHead
      while (converter ne null) {
        converter.write(buffer.text, event)
        converter = converter.getNext
      }
    }
  }
}
 */