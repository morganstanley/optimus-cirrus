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
package optimus.platform.util
import sun.tools.attach.HotSpotVirtualMachine

import java.io.InputStreamReader
import scala.util.control.NonFatal

trait JExecute {
  /*
  Isolate these references to s.t.attach in this class.
  Code that uses the InfoDump object will not be exposed at compile time to the HotspotVirtualMachine class.
   */

  import InfoDumpUtils._

  def jexecute(
      sb: StringBuilder,
      hsvm: HotSpotVirtualMachine,
      maxLines: Int,
      timeoutMs: Long,
      logging: Boolean,
      cmd: String,
      args: Any*): Int = {
    val isr =
      () =>
        try {
          new InputStreamReader(hsvm.executeCommand(cmd, args: _*))
        } catch {
          case NonFatal(_) =>
            return InfoDumpUtils.EXCEPTION
        }
    drain(sb, isr, s"[jcmd] $cmd ${args.mkString(" ")}", timeoutMs = timeoutMs, logging = logging, maxLines = maxLines)
  }

  def jexecute(sb: StringBuilder, hsvm: HotSpotVirtualMachine, cmd: String, args: Any*): Int =
    jexecute(sb, hsvm, maxLines = 0, timeoutMs = 30000L, logging = true, cmd, args: _*)

}
