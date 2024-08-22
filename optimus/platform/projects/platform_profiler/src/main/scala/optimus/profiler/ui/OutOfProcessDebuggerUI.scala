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
package optimus.profiler.ui

import java.rmi.ConnectException

import optimus.graph.JMXConnection
import optimus.graph.Settings
import optimus.graph.diagnostics.ProfilerUIControl

/*This application is to start the debugger UI in a separate process. Please specify in both here and the running
optimus application the JMX port and that they are running under out of process mode.
This can be done by setting outOfProcess flag to true and specify the clientPort on the optimus application.
 */
object OutOfProcessDebuggerUI extends App {
  /*this line is to avoid deadlock between node trace initializer and onModeChangeListeners*/
  Settings.ensureTraceAvailable()
  try {
    ProfilerUIControl.startGraphDebugger()
  } catch {
    case e: ConnectException =>
      println("Please double check that your remote application is running and the client port is correct.")
  }
  if (JMXConnection.graph.getConsoleFlag)
    ProfilerUIControl.brk()
}
