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
package optimus.exceptions

import java.lang.{Boolean => JBoolean}
import java.nio.charset.StandardCharsets

import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.CacheBuilder
import optimus.exceptions.config.RTListConfig
import optimus.utils.MiscUtils.Endoish._
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.NameTransformer

object RTException extends Throwable with RTExceptionTrait {
  override def equals(obj: Any): Boolean = obj match {
    case t: Throwable => RTList.isRT(t)
    case _            => false
  }
  //noinspection IfElseToFilterdOption
  def unapply(t: Throwable): Option[Throwable] = if (RTList.isRT(t)) Some(t) else None
}

object RTList {
  private val log = LoggerFactory.getLogger(this.getClass)
  final val ProxyConfig = "META-INF/optimus/exception-proxies.lst"
  final val ClassConfig = "META-INF/optimus/rt-exceptions.lst"

  @VisibleForTesting
  val proxyModules = {
    loadConfig(ProxyConfig) { line =>
      val moduleName = line + NameTransformer.MODULE_SUFFIX_STRING
      getClass.getClassLoader
        .loadClass(moduleName)
        .getDeclaredField(NameTransformer.MODULE_INSTANCE_NAME)
        .get(null)
        .asInstanceOf[ExceptionProxy]
    }
  }.boringApplyIf(RTListConfig.hasNoAdditions)(_ - AdditionalExceptionProxy)
  private val classesFromResources = loadConfig(ClassConfig)(identity)

  private def loadConfig[T](file: String)(parse: String => T): Set[T] = {
    val files = getClass.getClassLoader.getResources(file).asScala
    val lines = files.flatMap { url =>
      IOUtils.lineIterator(url.openStream(), StandardCharsets.UTF_8).asScala
    }
    lines.map(parse).toSet
  }

  //noinspection ExistsEquals (they're the same here, but we're being abusing equals so we may as well be explicit about it)
  private def isProxy(t: Throwable): Boolean = proxyModules.exists(_ == t)

  private val additions: Set[String] =
    RTListConfig.additionalRTExceptionClasses.map(_.split(';').toSet).getOrElse(Set.empty[String])
  private val subtractions: Set[String] =
    RTListConfig.removedRTExceptionClasses.map(_.split(';').toSet).getOrElse(Set.empty[String])
  private val adjustedMembers: Set[String] =
    RTListStatic.members ++ additions -- subtractions ++ classesFromResources

  private def isRTWithAdjustment(fqcn: String) = {
    val matched = adjustedMembers.contains(fqcn)
    if (matched && additions.contains(fqcn))
      log.warn(
        s"[RTList] added to allow-list dynamically: $fqcn " +
          "(via optimus.additional.rt.exception.classes)")
    if (!matched && subtractions.contains(fqcn))
      log.warn(
        s"[RTList] removed from allow-list dynamically: $fqcn " +
          "(via optimus.removed.rt.exception.classes)")
    matched
  }

  private val isRTCache = CacheBuilder.newBuilder().weakKeys().maximumSize(100000).build[Throwable, JBoolean]()
  def isRT(t: Throwable): Boolean = {
    var horror: Throwable = null // So we can throw the exception outside of the cache callback
    val ret = isRTCache.get(
      t, { () =>
        try {
          t.isInstanceOf[RTExceptionTrait] || t.isInstanceOf[RTExceptionInterface] ||
          isRTWithAdjustment(t.getClass.getCanonicalName) || isProxy(t)
        } catch {
          case e: Throwable =>
            horror = e
            false
        }
      }
    )
    if (horror ne null) {
      log.error(s"Got exception evaluating proxy $t; all proxies: $proxyModules", horror)
      throw horror
    } else ret
  }
}
