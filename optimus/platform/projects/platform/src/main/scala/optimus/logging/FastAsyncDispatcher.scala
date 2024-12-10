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

import java.util

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import msjava.logbackutils.async.AsyncDispatcherQueueImplPatch
import msjava.logbackutils.async.AsyncDispatcherQueueImplPatch.BaseDispatchThread

import scala.annotation.tailrec

class FastAsyncDispatcher extends AsyncDispatcherQueueImplPatch[ILoggingEvent] {

  var fastAppenders: List[WrappedAppender] = _

  override def setAppenders(appenderIterator: util.Iterator[Appender[ILoggingEvent]]): Unit = {
    import scala.jdk.CollectionConverters._
    super.setAppenders(appenderIterator)
    fastAppenders = appenderList.asScala.map {
      case alreadyFast: FastAppender => new WrappedFastAppender(alreadyFast)
      case slow =>
        addError(s"FastAsyncDispatcher expects a FastAppender not ${slow.getName} - $slow")
        new WrappedSlowAppender(slow)
    }.toList
  }
  @tailrec private def doCallAppenders(e: ILoggingEvent, appenders: List[WrappedAppender]): Unit = {
    if (!appenders.isEmpty) {
      try appenders.head.append(e)
      catch {
        case t: Throwable =>
          addError(t.getMessage, t)
      }
      doCallAppenders(e, appenders.tail)
    }
  }

  override protected def newDispatchThread(): BaseDispatchThread = {
    new FastDispatchThread()
  }
  class FastDispatchThread extends BaseDispatchThread {

    override def run(): Unit = {
      val pending = new util.ArrayList[ILoggingEvent](16)
      while (!isInterrupted) try {
        pending.clear()
        queue.drainTo(pending, 16)
        if (pending.isEmpty)
          processOne(queue.take())
        else {
          // unwound to avoid index creation
          val max = pending.size
          var index = 0
          while (index < max) {
            processOne(pending.get(index))
            index += 1
          }
        }
      } catch {
        case _: InterruptedException =>
          return
      }
    }

    private def processOne(e: ILoggingEvent) = {
      try doCallAppenders(e, fastAppenders)
      catch {
        case e: Throwable =>
          addError("A runtime exception occurred during logging.", e)
      } finally eventFlushed(e)
    }
  }
  abstract sealed class WrappedAppender {
    def append(event: ILoggingEvent): Unit
  }
  final class WrappedSlowAppender(slow: Appender[ILoggingEvent]) extends WrappedAppender {
    override def append(event: ILoggingEvent): Unit = slow.doAppend(event)
  }
  final class WrappedFastAppender(appender: FastAppender) extends WrappedAppender {
    val buffer = EncoderBuffer(appender.fastEncoder)
    override def append(event: ILoggingEvent): Unit = appender.append(event, buffer)
  }
}
 */