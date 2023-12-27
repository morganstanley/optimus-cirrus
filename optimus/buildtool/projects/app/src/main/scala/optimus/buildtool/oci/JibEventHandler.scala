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
package optimus.buildtool.oci

import java.util.function.Consumer

import com.google.cloud.tools.jib.api._
import com.google.cloud.tools.jib.event.events.ProgressEvent
import com.google.cloud.tools.jib.event.progress.Allocation
import msjava.slf4jutils.scalalog.Logger

/** Adapts Consumer[JibEvent] (what their API uses) into a more structured API. */
abstract class JibEventAdaptor extends Consumer[JibEvent] {
  final def accept(t: JibEvent) = t match {
    case le: LogEvent      => onLog(le.getLevel, le.getMessage)
    case pe: ProgressEvent => onProgress(pe.getAllocation, pe.getUnits)
    case _                 =>
  }

  protected def onLog(lvl: LogEvent.Level, msg: String): Unit = ()
  protected def onProgress(allocation: Allocation, units: Long): Unit = ()
}

final class JibEventHandler(logger: Logger, name: String) extends JibEventAdaptor {
  private def withName(msg: String): String = s"[$name] $msg"

  override def onLog(lvl: LogEvent.Level, msg: String): Unit = {
    import LogEvent.Level._
    lvl match {
      case ERROR                       => logger.error(withName(msg))
      case WARN                        => logger.warn(withName(msg))
      case INFO | LIFECYCLE | PROGRESS => logger.info(withName(msg))
      case DEBUG                       => logger.debug(withName(msg))
    }
  }

  override def onProgress(allocation: Allocation, units: Long): Unit = {
    val allocName = Iterator
      .iterate(allocation)(_.getParent.orElse(null))
      .takeWhile(_ != null)
      .map(_.getDescription)
      .toList
      .reverse // they come leaf -> root
      .mkString("->")
    // random message really; it could be anything
    logger.info(s"[$name][PROGRESS] $allocName: $units/${allocation.getAllocationUnits}")
  }
}
