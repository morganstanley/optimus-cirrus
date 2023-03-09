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
package optimus.utils

import java.nio.ByteBuffer
import java.nio.channels.ServerSocketChannel
import java.nio.file.WatchEvent

import scala.annotation.tailrec
import scala.util.Properties

object JavaVersionCompat {

  private val addressMethod = Class.forName("sun.nio.ch.DirectBuffer").getMethod("address")
  def directBuffer_address(b: ByteBuffer): Long = addressMethod.invoke(b).asInstanceOf[Long]

  private val finalRefCountMethod = Class.forName("jdk.internal.misc.VM").getMethod("getFinalRefCount")
  def getFinalRefCount: Int = finalRefCountMethod.invoke(null).asInstanceOf[Int]

  /**
   * @return
   *   the PlatformClassLoader of cl, if any. This is useful because on Java 9+ any classloader not (indirectly)
   *   parented to the PlatformClassLoader cannot load classes from any non-base Java modules. In Java 8 and earlier
   *   it's fine to use null as the parent for a classloader
   */
  @tailrec def platformClassLoader(cl: ClassLoader = getClass.getClassLoader): ClassLoader = {
    if (cl.getClass.getName == "jdk.internal.loader.ClassLoaders$PlatformClassLoader") cl
    else platformClassLoader(cl.getParent)
  }

  object WatchEventModifiers {
    private def lookup[T <: java.lang.Enum[T], V](cl: Class[_], nm: String): V = {
      java.lang.Enum.valueOf(cl.asInstanceOf[Class[T]], nm).asInstanceOf[V]
    }

    val highSensititivty: WatchEvent.Modifier =
      lookup(Class.forName("com.sun.nio.file.SensitivityWatchEventModifier"), "HIGH")
    val fileTree: WatchEvent.Modifier =
      lookup(Class.forName("com.sun.nio.file.ExtendedWatchEventModifier"), "FILE_TREE")
  }

  private val selChImplClass = Class.forName("sun.nio.ch.SelChImpl")
  private val selChImplKill = selChImplClass.getMethod("kill")
}

package compat {

  object VMSupport {
    private val vmSupportClass = Class.forName("jdk.internal.vm.VMSupport")
    private val getAgentPropertiesMethod = vmSupportClass.getMethod("getAgentProperties")
    def getAgentProperties() = getAgentPropertiesMethod.invoke(null).asInstanceOf[java.util.Properties]
  }

}
