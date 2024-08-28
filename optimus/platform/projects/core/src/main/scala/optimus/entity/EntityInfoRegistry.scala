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
package optimus.entity

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._
import optimus.platform.storable.StorableCompanionBase
import msjava.slf4jutils.scalalog._
import optimus.platform.storable.Entity

object EntityInfoRegistry {
  private[this] val log = getLogger(getClass)
  private[this] val sizeWarningLimit = 10000
  private[this] val storableCompanionSizeLimit = new SizeLimit(sizeWarningLimit, "Storable companion registry")
  private[this] val entityCompanionSizeLimit = new SizeLimit(sizeWarningLimit, "Entity companion registry")

  private[this] val storableCompanionRegistry = new ConcurrentHashMap[String, StorableCompanionBase[_]].asScala
  private[this] val entityCompanionRegistry = new ConcurrentHashMap[String, Entity].asScala
  private[this] val nameToClass = new ConcurrentHashMap[(String, ClassLoader), Class[_]].asScala

  def getClassInfo(className: String): ClassEntityInfo = getInfo(className).asInstanceOf[ClassEntityInfo]
  def getClassInfo(klass: Class[_]): ClassEntityInfo = getInfo(klass).asInstanceOf[ClassEntityInfo]

  def getInfo(className: String): StorableInfo = getCompanion(className).info
  def getInfo(klass: Class[_]): StorableInfo = getCompanion(klass).info

  def getCompanion(className: String): StorableCompanionBase[_] = {
    val loader = contextOrCodetreeClassLoader()
    val c = nameToClass.getOrElseUpdate(
      (className, loader), {
        Class.forName(className, true, loader)
      })
    getCompanion(c)
  }

  def getCompanion(klass: Class[_]): StorableCompanionBase[_] = {
    storableCompanionRegistry.getOrElseUpdate(
      klass.getName, {
        checkNotNested(klass) // if the class is nested then the companion must be too
        storableCompanionSizeLimit.incrementSize()
        val moduleClass: Class[_] = Class.forName(klass.getName + "$", true, klass.getClassLoader)
        moduleClass.getField("MODULE$").get(moduleClass).asInstanceOf[StorableCompanionBase[_]]
      }
    )
  }

  def getModule(klass: Class[_]): Entity =
    entityCompanionRegistry
      .getOrElseUpdate(
        klass.getName, {
          checkNotNested(klass)
          entityCompanionSizeLimit.incrementSize()
          klass.getField("MODULE$").get(klass).asInstanceOf[Entity]
        })

  def getModuleInfo(klass: Class[_]): EntityInfo = getModule(klass).$info

  // We can't instantiate nested companions without having first instantiated their enclosing class
  // But this doesn't matter for now because we don't support distributing inner entities
  private def checkNotNested(klass: Class[_]): Unit = {
    if (klass.getDeclaredFields.exists(_.getName.equals("$outer"))) {
      throw new IllegalArgumentException(s"Cannot instantiate companion for nested class ${klass.getName}")
    }
  }

  // There were cases where the entity info registry became very large due to traits containing nested entities being
  // mixed in to commonly instantiated entities. Moving entity infos to top level should improve this, as should
  // removing a reference to the module val from  module entity infos, but we leave this here as a warning in the
  // future should anyone else notice similar excessive memory consumption
  class SizeLimit(val initialLimit: Int, name: String) {
    private val currentSize: AtomicInteger = new AtomicInteger()
    private var currentLimit: Int = initialLimit

    def incrementSize(): Unit = {
      val newSize = currentSize.incrementAndGet()
      if (newSize > currentLimit) {
        log.warn(
          s"$name size($newSize) has exceeded the warning limit($currentLimit)." +
            s"This could cause excess memory consumption. Next warning at ${currentLimit * 2}")
        currentLimit *= 2
      }
    }
  }

  /**
   * ForkJoinPools now always use the system classloader by default. We try pretty hard to make sure that the context
   * classloader for FJP threads running Optimus code is the one that loaded codetree but just in case, here's a
   * fallback. (Using the system class loader would never be right.)
   */
  private[this] def contextOrCodetreeClassLoader(): ClassLoader = {
    val ccl = Thread.currentThread().getContextClassLoader
    if (ccl == ClassLoader.getSystemClassLoader) getClass.getClassLoader
    else ccl
  }
}
