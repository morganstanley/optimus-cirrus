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

import java.io.IOException
import java.util.concurrent.locks.ReentrantLock

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.Layout
import ch.qos.logback.core.OutputStreamAppender
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.spi.DeferredProcessingAware
import ch.qos.logback.core.spi.FilterReply
import ch.qos.logback.core.status.ErrorStatus
import ch.qos.logback.core.status.WarnStatus

import scala.runtime.BooleanRef

trait FastAppender {
  def append(event: ILoggingEvent, buffer: EncoderBuffer): Unit
  def fastEncoder: FastEncoder
}
trait FastOutputStreamAppender extends OutputStreamAppender[ILoggingEvent] with FastAppender {
  override def start(): Unit = {
    this.fastEncoder = _encoder.asInstanceOf[FastEncoder]
    super.start()
  }
  def _encoder: Encoder[ILoggingEvent]

  var fastEncoder: FastEncoder = _

  private var lockOutputStream: Boolean = false
  def setLockOutputStream(lockOutputStream: Boolean) = this.lockOutputStream = lockOutputStream
  private var preventReentry: Boolean = false
  def setPreventReentry(preventReentry: Boolean) = this.preventReentry = preventReentry
  private val fastGuard = ThreadLocal.withInitial[BooleanRef](() => new BooleanRef(false))
  private var statusRepeatCount = 0
  private var exceptionCount = 0
  private def ALLOWED_REPEATS = 3

  override def append(event: ILoggingEvent, buffer: EncoderBuffer): Unit = {
    def realAppend: Unit = {
      try {
        if (!isStarted) {
          if (statusRepeatCount < ALLOWED_REPEATS) {
            statusRepeatCount += 1
            addStatus(new WarnStatus(s"Attempted to append to non started appender [$getName].", this))
          }
        } else if (getFilterChainDecision(event) ne FilterReply.DENY) {
          // ok, we now do the real work

          // this step avoids LBCLASSIC-139
          event match {
            case aware: DeferredProcessingAware =>
              aware.prepareForDeferredProcessing()
            case _ =>
          }
          fastEncoder.encodeToBuffer(event, buffer)
          writeBytes(buffer)
        }
      } catch {
        case ioe: IOException =>
          // as soon as an exception occurs, move to non-started state
          // and add a single ErrorStatus to the SM.
          _started(false)
          addStatus(new ErrorStatus("IO failure in appender", this, ioe))
        case e: Exception =>
          if (exceptionCount < ALLOWED_REPEATS) {
            exceptionCount += 1
            addError(s"Appender [$getName] failed to append.", e)
          }
      }
    }
    // WARNING: The guard check MUST be the first statement in the
    // doAppend() method.
    // prevent re-entry.
    if (!preventReentry) realAppend
    else {
      val g = fastGuard.get
      if (!g.elem) {
        g.elem = true
        try realAppend
        finally g.elem = false
      }

    }
  }
  private def writeBytes(buffer: EncoderBuffer): Unit = {
    def doWrite = {
      buffer.writeTo(getOutputStream)
      if (isImmediateFlush) getOutputStream.flush()
    }
    if (!buffer.isEmpty) {
      if (lockOutputStream) {
        val lock = _lock
        lock.lock()
        try doWrite
        finally if (lockOutputStream) lock.unlock()
      } else doWrite
    }
  }

  override def setLayout(layout: Layout[ILoggingEvent]): Unit = {
    addWarn("This appender no longer admits a layout as a sub-component, set an encoder instead.")
    addWarn("To ensure compatibility, wrapping your layout in FastEncoder.")
    addWarn("See also " + CoreConstants.CODES_URL + "#layoutInsteadOfEncoder for details")
    val lwe = new FastEncoder
    lwe.setLayout(layout)
    lwe.setContext(getContext)
    setEncoder(lwe)
  }

  override def setEncoder(encoder: Encoder[ILoggingEvent]): Unit = {
    encoder match {
      case fast: FastEncoder =>
        super.setEncoder(fast)
        this.fastEncoder = fast
      case _ =>
        addError(s"This appender is a FastAppender, and requires a FastEncoder, not a $encoder")
    }

  }
  protected def _lock: ReentrantLock
  protected def _started(value: Boolean): Unit
}

final class FastConsoleAppender extends ConsoleAppender[ILoggingEvent] with FastOutputStreamAppender {
  override def start(): Unit = {
    super.start()
  }
  override protected final def _lock: ReentrantLock = ??? // lock
  override protected final def _started(value: Boolean): Unit = started = value
  override def _encoder: Encoder[ILoggingEvent] = encoder
}
