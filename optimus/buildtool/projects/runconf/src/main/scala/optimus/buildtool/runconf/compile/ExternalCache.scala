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
package optimus.buildtool.runconf.compile

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.Config
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ParentId
import optimus.buildtool.runconf.RunConf
import optimus.buildtool.runconf.RunConfId
import optimus.buildtool.runconf.ScopedName
import optimus.buildtool.runconf.Template

import scala.collection.immutable.Seq

// invalidates whole cache if base key changes
// for example if syntheticProperties change, we don't want to cache any config that was resolved based on them
class BaseKeyCache[BaseKey, Key, Value] {
  private var baseKey: BaseKey = _
  private val cache = new ConcurrentHashMap[Key, Value]()

  def cached(baseKey: BaseKey, key: Key)(compute: => Value): Value = {
    if (this.baseKey != baseKey) {
      synchronized {
        if (this.baseKey != baseKey) {
          this.baseKey = baseKey
        }
      }
      cache.clear()
    }
    cache.computeIfAbsent(key, _ => compute)
  }
}

// evicts entry from cache based on eviction key
// for example we would like to hold Config parsed from InputFile, but only hold one entry for one actual file
// as without this eviction, cache would store all versions of file that is edited forever.
class EvictingCache[EvictionKey, Key, Value] {
  private val cache = new ConcurrentHashMap[EvictionKey, (Key, Value)]()

  def cached(evictionKey: EvictionKey, key: Key)(compute: => Value): Value = {
    def recompute(): Value = {
      val value = compute
      cache.put(evictionKey, (key, value))
      value
    }

    cache.get(evictionKey) match {
      case null =>
        recompute()
      case (cachedKey, cachedValue) =>
        if (safeEquals(key, cachedKey)) cachedValue else recompute()
    }
  }

  def clear(): Unit = cache.clear()

  // Config.equals actually can throw if config has a key that can't be resolved
  private def safeEquals(a: Key, b: Key): Boolean = {
    try a == b
    catch { case _: Exception => false }
  }

}

// combination of the two above, allows to evict cache entries by key, but also clears the cache if baseKey changes
class BaseKeyEvictingCache[BaseKey, EvictionKey, Key, Value] {
  private var baseKey: BaseKey = _
  private val cache = new EvictingCache[EvictionKey, Key, Value]

  def cached(baseKey: BaseKey, evictionKey: EvictionKey, key: Key)(compute: => Value): Value = {
    if (this.baseKey != baseKey) {
      synchronized {
        if (this.baseKey != baseKey) {
          this.baseKey = baseKey
        }
      }
      cache.clear()
    }
    cache.cached(evictionKey, key)(compute)
  }
}

class ExternalCache {
  def evictingCache[EvictionKey, Key, Value] = new EvictingCache[EvictionKey, Key, Value]
  def baseKeyCache[BaseKey, Key, Value] = new BaseKeyCache[BaseKey, Key, Value]
  def baseKeyEvictingCache[BaseKey, EvictionKey, Key, Value] =
    new BaseKeyEvictingCache[BaseKey, EvictionKey, Key, Value]

  val resolveWithAppliedScopes = baseKeyCache[Config, (Config, ParentId), Config]
  val parse = evictingCache[Path, InputFile, Either[(String, Int), Config]]
  val merge = evictingCache[Option[ParentId], Seq[Config], Config]
  val javaOpts = evictingCache[(ScopedName, Option[String]), Seq[String], Seq[String]]
  val installPath = baseKeyCache[(RunEnv, Option[Path]), MetaBundle, String]
  val appDir = baseKeyCache[(RunEnv, Option[Path]), MetaBundle, String]
  val scopeDependentResolutionTemplate = baseKeyEvictingCache[(RunEnv, Option[Path]), ScopedName, Template, Template]
  val scopeDependentResolutionRunConf = baseKeyEvictingCache[(RunEnv, Option[Path]), RunConfId, RunConf, RunConf]
  val applyParents = evictingCache[ScopedName, Seq[UnresolvedRunConf], UnresolvedRunConf]
  val typer = evictingCache[ScopedName, RawProperties, Either[Seq[Problem], Unit]]
  val buildUnresolved = evictingCache[ScopedName, (Block, RawProperties), UnresolvedRunConf]
}

object NoCache extends ExternalCache {
  override def evictingCache[EvictionKey, Key, Value] = {
    new EvictingCache[EvictionKey, Key, Value] {
      override def cached(evictionKey: EvictionKey, key: Key)(compute: => Value): Value = compute
    }
  }
  override def baseKeyCache[BaseKey, Key, Value] = {
    new BaseKeyCache[BaseKey, Key, Value] {
      override def cached(baseKey: BaseKey, key: Key)(compute: => Value): Value = compute
    }
  }
  override def baseKeyEvictingCache[BaseKey, EvictionKey, Key, Value] = {
    new BaseKeyEvictingCache[BaseKey, EvictionKey, Key, Value] {
      override def cached(baseKey: BaseKey, evictionKey: EvictionKey, key: Key)(compute: => Value): Value = compute
    }
  }
}
