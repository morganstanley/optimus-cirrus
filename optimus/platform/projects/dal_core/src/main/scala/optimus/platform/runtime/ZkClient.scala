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
package optimus.platform.runtime

import java.io.Closeable

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.RemovalListener
import com.google.common.cache.RemovalNotification
import msjava.zkapi.internal.ZkaContext

trait ZkClient extends Closeable

trait ZkClientFactory[T <: ZkClient] {
  protected type CacheKeyType <: AnyRef
  protected final type CacheValueType = T

  def createClient(ck: CacheKeyType): CacheValueType

  private val clientRemovalListener = new RemovalListener[CacheKeyType, CacheValueType] {
    override def onRemoval(notification: RemovalNotification[CacheKeyType, CacheValueType]): Unit = {
      // Close ZkClient as it is no longer going to be used..
      notification.getValue.close()
    }
  }

  private val cacheLoader = new CacheLoader[CacheKeyType, CacheValueType] {
    override def load(k: CacheKeyType): CacheValueType = {
      createClient(k)
    }
  }

  private[this] val clients = {
    CacheBuilder
      .newBuilder()
      .removalListener(clientRemovalListener)
      .build[CacheKeyType, CacheValueType](cacheLoader)
  }

  def getClient(k: CacheKeyType): CacheValueType =
    clients.get(k)

  def reset(): Unit = {
    clients.invalidateAll()
  }
}
