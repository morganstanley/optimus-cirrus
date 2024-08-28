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
package optimus.core.utils
import java.lang.invoke.MethodHandles
import java.util.concurrent.ConcurrentHashMap
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import msjava.slf4jutils.scalalog.getLogger
import optimus.core.MonitoringBreadcrumbs

/**
 * This utility allows you to cache and reuse a runtime mirror for a given class loader so that you don't have to
 * recompute it every time as they are quite expensive.
 */
object RuntimeMirror {

  private val mirrors = new ConcurrentHashMap[ClassLoader, RuntimeMirror]()

  def forClass(cls: Class[_]): Mirror = mirrorForClass(cls).mirror
  def mirrorForClass(cls: Class[_]): RuntimeMirror = {
    // getClassLoader returns null when the bootstrap class loader was used
    // in those cases we use the system class loader instead
    val classLoader = Option(cls.getClassLoader).getOrElse(ClassLoader.getSystemClassLoader)
    mirrorForClassLoader(classLoader)
  }

  def forClassLoader(loader: ClassLoader): Mirror = mirrorForClassLoader(loader).mirror
  def mirrorForClassLoader(classLoader: ClassLoader): RuntimeMirror =
    mirrors.computeIfAbsent(classLoader, new RuntimeMirror(_))

  def moduleByName(originalName: String, cls: Class[_], cached: Boolean = true): AnyRef =
    mirrorForClass(getClass).moduleByName(originalName, cls, cached)
}

class RuntimeMirror(classLoader: ClassLoader) {

  private val log = getLogger(this)

  val mirror: Mirror = universe.runtimeMirror(classLoader)
  private val modules = new ConcurrentHashMap[String, AnyRef]()

  /** To replace slow Scala reflection (calling class needs to be passed in case of fallback) */
  def moduleByName(originalName: String, callingClass: Class[_], cached: Boolean = true): AnyRef = {
    if (cached) {
      val existing = modules.get(originalName)
      if (existing eq null) {
        val newValue = getModuleByName(originalName, callingClass)
        modules.putIfAbsent(originalName, newValue) // don't crash while CHM is holding a lock (in a graph thread)
        newValue
      } else existing
    } else getModuleByName(originalName, callingClass)
  }

  private def getModuleByName(originalName: String, callingClass: Class[_]): AnyRef =
    try searchForModule(originalName)
    catch {
      case _: Exception =>
        try searchForInnerModule(originalName)
        catch {
          case ex: Exception =>
            log.debug(s"Couldn't find module $originalName with Java reflection, trying Scala", ex)
            scalaFallback(originalName, callingClass)
        }
    }

  private def searchForModule(originalName: String): AnyRef = {
    val name = if (originalName.endsWith("$")) originalName else originalName + "$"
    val c = classLoader.loadClass(name)
    val m = MethodHandles.lookup.findStaticGetter(c, "MODULE$", c) // NoSuchField
    m.invoke()
  }

  // trying for inner classes, where the inner class separator in Java is $, not .
  private def searchForInnerModule(originalName: String): AnyRef = {
    val innerName = originalName.patch(originalName.lastIndexOf("."), "$", 1)
    searchForModule(innerName)
  }

  // if Java doesn't work, try Scala [SEE_INNER_EMBEDDABLE_TRAIT]
  private val picklerClsName = "optimus.platform.pickling.PicklingReflectionUtils$"
  private def scalaFallback(name: String, callingClass: Class[_]): AnyRef = {
    val module = mirror.staticModule(name)
    val obj = mirror.reflectModule(module)
    // sending crumbs for cases where scala found the module, but not java.
    // We are intentionally ignoring pickler related events since it produces lots and lots of crumbs
    // and we are mainly interested in spotting new non-pickler uses.
    if (callingClass.getName != picklerClsName)
      MonitoringBreadcrumbs.sendScalaReflectionFallbackCrumb(name, callingClass.getName, "ScalaReflectionFallback")
    obj.instance.asInstanceOf[AnyRef]
  }
}
