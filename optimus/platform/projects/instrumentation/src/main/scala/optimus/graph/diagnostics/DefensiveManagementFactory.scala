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

import java.lang.management.ManagementFactory

import com.sun.management.OperatingSystemMXBean
import javax.management.ObjectName

import scala.util.Properties

class DefensiveOperatingSystemMXBean extends OperatingSystemMXBean {
  // There is a race condition in the static initialization of the OperatingSystemMXBean that manifests itself by a null pointer exception
  // when creating it or accessing one of it's methods. Sadly, this seemingly *only* happens within containers, and very very rarely.
  // After trying for several days to come up with a solid reproduction of the problem, I gave up and decided to code defensively around
  // the issue.

  // This problem only occurs on JDK-17, and this class should be removed when JDK 17 is obsolete or the problem is fixed upstream. This is slower
  // than creating the actual Bean because the bean is re-created on every call but given that only OBT uses 17 that should be ok...

  private def getBeanMethod[T](getterFunction: OperatingSystemMXBean => T): Option[T] = {
    try {
      val bean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
      Some(getterFunction(bean))
    } catch {
      case e: NullPointerException =>
        // don't really want to log here as this might create more weird static initialization business... should be rare anyway.
        println("NPE when accessing OS bean")
        None
    }
  }
  private def getNumber[T: Numeric](getter: OperatingSystemMXBean => T): T =
    getBeanMethod(getter).getOrElse(implicitly[Numeric[T]].zero)
  private def getString(getter: OperatingSystemMXBean => String): String = getBeanMethod(getter).getOrElse("")

  override def getCommittedVirtualMemorySize: Long = getNumber(_.getCommittedVirtualMemorySize)
  override def getTotalSwapSpaceSize: Long = getNumber(_.getTotalSwapSpaceSize)
  override def getFreeSwapSpaceSize: Long = getNumber(_.getFreeSwapSpaceSize)
  override def getProcessCpuTime: Long = getNumber(_.getProcessCpuTime)
  override def getFreePhysicalMemorySize: Long = getNumber(_.getFreePhysicalMemorySize)
  override def getTotalPhysicalMemorySize: Long = getNumber(_.getTotalPhysicalMemorySize)
  override def getSystemCpuLoad: Double = getNumber(_.getSystemCpuLoad)
  override def getProcessCpuLoad: Double = getNumber(_.getProcessCpuLoad)
  override def getName: String = getString(_.getName)
  override def getArch: String = getString(_.getArch)
  override def getVersion: String = getString(_.getVersion)
  // the main danger is that we will use the 0 default value to set the number of graph threads.
  override def getAvailableProcessors: Int = getNumber(_.getAvailableProcessors)
  override def getSystemLoadAverage: Double = getNumber(_.getSystemLoadAverage)
  override def getObjectName: ObjectName = new ObjectName(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME)
}

object DefensiveManagementFactory {
  private def useDefensiveBean: Boolean = {
    val version = Properties.javaVmVersion
    // these are the version that use CgroupsV1SubsystemController, which we know is the source of the error
    version.startsWith("18.") || version.startsWith("17.") || version.startsWith("16.") || version.startsWith("15.")
  }
  def getOperatingSystemMXBean(): OperatingSystemMXBean =
    if (useDefensiveBean) new DefensiveOperatingSystemMXBean()
    else ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
}
