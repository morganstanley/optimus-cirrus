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
package optimus.stratosphere.utils

import optimus.stratosphere.bootstrap.OsSpecific

import scala.Console._
import scala.collection.immutable.Seq
import scala.util.control.NonFatal

object ProcessUtils {

  /** print on console using red ascii color and clear formatting to default */
  def printMarkedRed(text: String) = s"$RED$text$RESET"

  /** print on console using yellow ascii color and clear formatting to default */
  def printMarkedYellow(text: String) = s"$YELLOW$text$RESET"

  def addOptimusAppOptions(pb: ProcessBuilder, jvmOpts: Seq[String]): ProcessBuilder = {
    val env = pb.environment()
    val existingOpts = Option(env.get("OPTIMUSAPP_OPTS")).toSeq.flatMap(_.split(" "))
    val opts = jvmOpts.filter(_.nonEmpty) ++ existingOpts
    if (opts.nonEmpty) env.put("OPTIMUSAPP_OPTS", opts.mkString(" "))
    pb
  }

  def setupTerminal(): Unit = if (OsSpecific.isWindows) {
    try {
      // Set output mode to handle virtual terminal sequences, which allows windows consoles to handle ANSI codes
      import com.sun.jna.Function
      import com.sun.jna.platform.win32.WinDef
      import com.sun.jna.platform.win32.WinDef._
      import com.sun.jna.platform.win32.WinNT.HANDLE

      val STD_OUTPUT_HANDLE = new DWORD(-11)
      val P_MODE = new WinDef.DWORDByReference(new DWORD(0))
      val ENABLE_VIRTUAL_TERMINAL_PROCESSING = 4
      val getHandle = Function.getFunction("kernel32", "GetStdHandle")

      val stdOut = getHandle.invoke(classOf[HANDLE], Array(STD_OUTPUT_HANDLE))
      val getConsoleMode: Function = Function.getFunction("kernel32", "GetConsoleMode")
      getConsoleMode.invoke(classOf[BOOL], Array(stdOut, P_MODE))
      val dwMode = P_MODE.getValue
      dwMode.setValue(dwMode.intValue | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
      val setConsoleMode: Function = Function.getFunction("kernel32", "SetConsoleMode")
      setConsoleMode.invoke(classOf[BOOL], Array(stdOut, dwMode))
    } catch {
      case NonFatal(_) => // do nothing
    }
  }

}
