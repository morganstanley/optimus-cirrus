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
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.utils.MemSize

import java.lang.management.ManagementFactory
import java.net.InetAddress
import scala.concurrent.duration._

object EnvironmentUtils {

  lazy val userName: String = System.getProperty("user.name")

  lazy val hostName: String = InetAddress.getLocalHost.getHostName

  private def system =
    ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean]

  // note that OperatingSystemMXBean#getTotal/FreePhysicalMemorySize is deprecated but getTotal/MemorySize is
  // documented to return the same value
  def getTotalPhysicalMemorySize: MemSize = MemSize.of(system.getTotalMemorySize)
  def getFreePhysicalMemorySize: MemSize = MemSize.of(system.getFreeMemorySize)

  def getFreeSwapMemorySize: MemSize = MemSize.of(system.getFreeSwapSpaceSize)

  def availableProcessors(): Int = Runtime.getRuntime.availableProcessors()

  def isProid(ws: StratoWorkspaceCommon): Boolean = {
    val proidTypeMarker = "G"
    val knownProids = ws.internal.environment.proids
    val isKnownProid = knownProids.contains(ws.userName)

    if (OsSpecific.isWindows || isKnownProid) isKnownProid
    else
      CommonProcess
        .in(ws)
        .runAndWaitFor(Seq("phone", "-bw", "-type", ws.userName), timeout = 5.seconds)
        .endsWith(proidTypeMarker)
  }
}
