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
package optimus.utils.zookeeper

import java.io.Closeable
import msjava.slf4jutils.scalalog.getLogger
import optimus.utils.curator.DeprecatedNodeCache
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.curator.framework.recipes.cache.NodeCacheListener

import scala.ref.WeakReference

private[optimus] object ReadOnlyDistributedValue {
  val log = getLogger[ReadOnlyDistributedValue.type]
}

abstract class ReadOnlyDistributedValue[T](
    curator: CuratorFramework,
    path: String,
    transformer: Array[Byte] => T,
    weak: Boolean = false)
    extends Closeable {
  // can be overwritten for testing
  def onNodeChange(v: Option[T]): Unit

  private val cache = new DeprecatedNodeCache(curator, path)

  if (weak)
    cache.getListenable.addListener(WeakNodeCacheListener(cache, this))
  else
    cache.getListenable.addListener(new NodeCacheListener {
      override def nodeChanged(): Unit = {
        onNodeChange(value)
      }
    })
  cache.start(true)

  def value = Option(cache.getCurrentData) map { node =>
    transformer(node.getData)
  }

  def close(): Unit = cache.close()
}

private class WeakNodeCacheListener[T] private (
    val cache: DeprecatedNodeCache,
    val value: WeakReference[ReadOnlyDistributedValue[T]])
    extends NodeCacheListener {
  def nodeChanged(): Unit = {
    value match {
      case WeakReference(v) => v.onNodeChange(v.value)
      case _                => cache.close()
    }
  }
}

private object WeakNodeCacheListener {
  def apply[T](cache: DeprecatedNodeCache, value: ReadOnlyDistributedValue[T]): NodeCacheListener = {
    new WeakNodeCacheListener[T](cache, WeakReference(value))
  }
}
