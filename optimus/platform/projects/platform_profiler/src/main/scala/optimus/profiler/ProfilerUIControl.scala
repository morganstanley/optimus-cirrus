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
package optimus.profiler

import optimus.utils.misc.Color
import java.awt.Rectangle
import java.awt.Robot
import java.awt.Toolkit
import java.io.File

import javax.imageio.ImageIO
import optimus.graph.NodeTask
import optimus.graph.diagnostics.PNodeInvalidate
import optimus.graph.diagnostics.GivenBlockProfile
import optimus.graph.diagnostics.WaitProfile
import optimus.graph.diagnostics.{ProfilerUIControl => ProfilerUIControlAPI}
import optimus.graph.outOfProcess.views.ScenarioStackProfileView
import optimus.graph.outOfProcess.views.ScenarioStackProfileViewHelper
import optimus.platform.ScenarioStack
import optimus.profiler.ui.GraphConsole
import optimus.profiler.ui.GraphDebuggerUI
import optimus.profiler.ui.browser.NodeTreeBrowser

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * Implementation class linking profiler UI methods to `core`.
 * @see
 *   [[java.util.ServiceLoader]]
 */
private final class ProfilerUIControl extends ProfilerUIControlAPI {
  override def startGraphDebugger(): Unit = GraphDebuggerUI.start()

  override def getInvalidates: ArrayBuffer[PNodeInvalidate] = ProfilerUI.getInvalidates
  override def getSSRoots: Iterable[ScenarioStackProfileView] = ProfilerUI.getSSRoots
  override def getWaits: ArrayBuffer[WaitProfile] = ProfilerUI.getWaits
  override def createScenarioStackProfileView(ss: Iterable[GivenBlockProfile]) =
    ScenarioStackProfileViewHelper.createScenarioStackProfileView(ss)
  override def refresh(): Unit = GraphDebuggerUI.refresh()

  override def bookmark(name: String, color: Color): Unit = DebuggerUI.bookmark(name, color)
  override def brk(): Unit = DebuggerUI.brk()
  override def callback(name: String, x: Any): Any = DebuggerUI.callback(name, x)
  override def resetSStackUsage(): Unit = ProfilerUI.resetSStackUsage()
  override def consoleGo(): Unit = DebuggerUI.sgo()
  override def brk(msg: String): Unit = DebuggerUI.brk(msg)
  override def addTreeBrowserTab(title: String, ntsk: NodeTask): Unit =
    GraphDebuggerUI.addTab(title, NodeTreeBrowser(ntsk))

  override def formatName(ntsk: NodeTask): String = NodeFormatUI.formatName(ntsk)
  override def outputOnConsole(out: String): Unit = GraphConsole.out(out)
  override def screenshot(name: String): Unit = {
    val filename = s"debugger-screenshot-$name.png"
    val screenSize = Toolkit.getDefaultToolkit.getScreenSize
    val rectangle = new Rectangle(0, 0, screenSize.width, screenSize.height)
    val bufferedImage = (new Robot).createScreenCapture(rectangle)
    ImageIO.write(bufferedImage, "png", new File(filename))
  }

  override def underStackOf[T](ssIn: ScenarioStack)(f: => T): T = DebuggerUI.underStackOf(ssIn)(f)
}
