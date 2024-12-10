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

import java.nio.charset.Charset

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Layout
import ch.qos.logback.core.encoder.LayoutWrappingEncoder
import ch.qos.logback.core.pattern.PatternLayoutEncoderBase
object foo extends LayoutWrappingEncoder

class FastEncoder extends PatternLayoutEncoderBase[ILoggingEvent] {
  // private var fastLayout: FastPatternLayout = _

  // start from parent for comparison
  private def convertToBytes(s: String): Array[Byte] = {
    if (getCharset == null)
      s.getBytes
    else s.getBytes(getCharset)
  }

  override def encode(event: ILoggingEvent): Array[Byte] = {
    val txt: String = layout.doLayout(event)
    convertToBytes(txt)
  }
  // end from parent

  def encodeToBuffer(event: ILoggingEvent, buffer: EncoderBuffer) = {
    // fastLayout.doLayout(event, buffer)
  }

  override def setLayout(layout: Layout[ILoggingEvent]): Unit = {
    layout match {
      /* case fastPatternLayout: FastPatternLayout =>
        this.layout = fastPatternLayout
        this.fastLayout = fastPatternLayout */
      case _ =>
        addError(s"A FastEncoder requires a FastLayout not a $layout")
    }
  }
  override def start(): Unit = {
    /* fastLayout.setContext(context)
    fastLayout.start() */
    super.start()
  }
}
