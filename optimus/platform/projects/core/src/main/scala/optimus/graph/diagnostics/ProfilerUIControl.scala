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
package optimus.graph.diagnostics

import optimus.graph.NodeTask
import optimus.graph.outOfProcess.views.ScenarioStackProfileView
import optimus.platform.ScenarioStack
import optimus.platform.util.RuntimeServiceLoader
import optimus.utils.misc.Color

import scala.collection.mutable.ArrayBuffer

private[optimus] trait ProfilerUIControl {
  def startGraphDebugger(): Unit

  def brk(): Unit

  def brk(msg: String): Unit

  def bookmark(name: String, color: Color): Unit

  def callback(name: String, x: Any): Any

  def createScenarioStackProfileView(ss: Iterable[GivenBlockProfile]): Iterable[ScenarioStackProfileView]

  def consoleGo(): Unit

  def resetSStackUsage(): Unit

  def refresh(): Unit

  def formatName(ntsk: NodeTask): String

  def addTreeBrowserTab(title: String, ntsk: NodeTask): Unit

  def outputOnConsole(output: String): Unit

  def screenshot(name: String): Unit

  def underStackOf[T](ssIn: ScenarioStack)(f: => T): T

  def getWaits: ArrayBuffer[WaitProfile]

  def getSSRoots: Iterable[ScenarioStackProfileView]

  def getInvalidates: ArrayBuffer[PNodeInvalidate]
}

object ProfilerUIControl extends RuntimeServiceLoader[ProfilerUIControl] {
  implicit def asUIControl(thiz: ProfilerUIControl.type): ProfilerUIControl = service
}
