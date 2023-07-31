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

import com.sun.jna.Pointer
import com.sun.jna.platform.win32.User32
import com.sun.jna.platform.win32.WinDef
import com.sun.jna.platform.win32.WinUser
import com.sun.jna.ptr.IntByReference

object WindowsUtils {
  def hasVisibleWindow(process: ProcessHandle): Boolean = {
    var visibleWindowFound = false

    User32.INSTANCE.EnumWindows(
      new WinUser.WNDENUMPROC() {
        override def callback(windowHandle: WinDef.HWND, pointer: Pointer): Boolean = {
          val procId: IntByReference = new IntByReference
          User32.INSTANCE.GetWindowThreadProcessId(windowHandle, procId)
          if (procId.getValue == process.pid() && User32.INSTANCE.IsWindowVisible(windowHandle))
            visibleWindowFound = true
          true
        }
      },
      Pointer.NULL
    )
    visibleWindowFound
  }
}
