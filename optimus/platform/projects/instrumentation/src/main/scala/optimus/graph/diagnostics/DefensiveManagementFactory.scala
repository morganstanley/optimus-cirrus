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
import optimus.graph.DiagnosticSettings
import optimus.utils.MiscUtils

import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.management.ObjectName

object DefensiveOperatingSystemMXBean {
  private val errors = new AtomicInteger(0)
  private val firstTime = new AtomicBoolean(true)

  private val lookup = MethodHandles.publicLookup
  private val mt = MethodType.methodType(classOf[Double])
  private val getCpuLoadMH =
    try {
      lookup.findVirtual(classOf[OperatingSystemMXBean], "getCpuLoad", mt)
    } catch {
      case _: NoSuchMethodException =>
        lookup.findVirtual(classOf[OperatingSystemMXBean], "getSystemCpuLoad", mt)
    }
  def getCpuLoad(b: OperatingSystemMXBean): Double = getCpuLoadMH.invoke(b)
}

final class DefensiveOperatingSystemMXBean extends OperatingSystemMXBean {
  import DefensiveOperatingSystemMXBean._
  /* There is a race condition in the static initialization of the OperatingSystemMXBean that manifests itself by a null pointer exception
      when creating it or accessing one of its methods. Sadly, this seemingly *only* happens within containers, and very, very rarely.
      It will manifest as a NPE looking something like this:
  at java.base/java.util.Objects.requireNonNull(Objects.java:233)
	at java.base/sun.nio.fs.UnixFileSystem.getPath(UnixFileSystem.java:297)
	at java.base/java.nio.file.Path.of(Path.java:148)
	at java.base/java.nio.file.Paths.get(Paths.java:69)
	at java.base/jdk.internal.platform.CgroupUtil.lambda$readStringValue$1(CgroupUtil.java:67)
	at java.base/java.security.AccessController.doPrivileged(AccessController.java:571)
	at java.base/jdk.internal.platform.CgroupUtil.readStringValue(CgroupUtil.java:69)
	at java.base/jdk.internal.platform.CgroupSubsystemController.getStringValue(CgroupSubsystemController.java:65)
	at java.base/jdk.internal.platform.cgroupv1.CgroupV1Subsystem.getCpuSetCpus(CgroupV1Subsystem.java:275)
	at java.base/jdk.internal.platform.CgroupMetrics.getCpuSetCpus(CgroupMetrics.java:100)
	at jdk.management/com.sun.management.internal.OperatingSystemImpl.isCpuSetSameAsHostCpuSet(OperatingSystemImpl.java:277)
	at jdk.management/com.sun.management.internal.OperatingSystemImpl$ContainerCpuTicks.getContainerCpuLoad(OperatingSystemImpl.java:96)
	at jdk.management/com.sun.management.internal.OperatingSystemImpl.getProcessCpuLoad(OperatingSystemImpl.java:271)

	https://bugs.openjdk.org/browse/JDK-8272124 implies that this problem only occurred through JDK-17, but there are multiple
	conceivable ways that jdk.internal.platform.cgroupv1.CgroupV1SubsystemController.path for the controller passed to
	jdk.internal.platform.CgroupUtil.readStringValue might turn out to be null, and unfortunately the initialization of
	CgroupV1SubsystemController does not even occur on the same stack.
   */

  if (firstTime.getAndSet(false)) {
    // force the bean to read /proc/self/stat now.
    // The underlying bean method might NPE as described above, causing our wrapper to return 0.
    getSystemCpuLoad()
    getProcessCpuLoad()
  }

  private def getBeanMethod[T](getterFunction: OperatingSystemMXBean => T): Option[T] = {
    try {
      val bean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
      Some(getterFunction(bean))
    } catch {
      case e: NullPointerException =>
        // don't really want to log here as this might create more weird static initialization business... should be rare anyway.
        if (MiscUtils.isPowerOfTwo(errors.incrementAndGet())) {
          println(
            s"Caught NPE accessing OperatingSystemMXBean $errors - this message and stack trace are for information only!")
          e.printStackTrace()
        }
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
  def getCpuLoad: Double = getNumber(DefensiveOperatingSystemMXBean.getCpuLoad(_))
  def getSystemCpuLoad: Double = getCpuLoad
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
  private def useDefensiveBean: Boolean =
    DiagnosticSettings.getBoolProperty("optimus.defensive.OperatingSystemMXBean", true)
  def getOperatingSystemMXBean(): OperatingSystemMXBean =
    if (useDefensiveBean) new DefensiveOperatingSystemMXBean()
    else ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
}
