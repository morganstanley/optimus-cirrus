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
package optimus.dsi.util

import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

import com.google.common.base.Ticker
import com.google.common.cache.CacheBuilder
import com.google.common.cache.RemovalCause
import com.google.common.cache.RemovalListener
import com.google.common.cache.RemovalNotification

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait Expirable {
  def onExpiration(): Unit
}

/*
  This class provides a functionality of notifying entries when they expire. If the entry is removed explicitly, then no
  notification is sent. Notifications are processed in a separate threadpool.
 */
class ExpirableMap[K <: Object, V <: Expirable](
    executor: ScheduledThreadPoolExecutor,
    expiryIntervalInMillis: Int,
    refreshEntryAtEveryAccess: Boolean,
    ticker: Option[Ticker]) { // Ticker is needed for Tests

  def this(executor: ScheduledThreadPoolExecutor, expiryIntervalInMillis: Int, refreshEntryAtEveryAccess: Boolean) =
    this(executor, expiryIntervalInMillis, refreshEntryAtEveryAccess, None)

  private[this] val ctx = ExecutionContext.fromExecutor(executor)

  private[this] val builder = CacheBuilder
    .newBuilder()
    .removalListener(new RemovalListener[K, V] {
      override def onRemoval(notification: RemovalNotification[K, V]) = {
        if (notification.getCause == RemovalCause.EXPIRED) {
          Future {
            notification.getValue.onExpiration()
          }(ctx)
        }
      }
    })

  ticker foreach { x =>
    builder.ticker(x)
  }
  if (refreshEntryAtEveryAccess)
    builder.expireAfterAccess(expiryIntervalInMillis, TimeUnit.MILLISECONDS)
  else
    builder.expireAfterWrite(expiryIntervalInMillis, TimeUnit.MILLISECONDS)

  private[this] val cache = builder.build[K, V]()

  // schedule cleanup of expired entries at regular interval
  executor.scheduleAtFixedRate(
    new Runnable {
      override def run(): Unit = {
        cache.cleanUp()
      }
    },
    expiryIntervalInMillis,
    expiryIntervalInMillis,
    TimeUnit.MILLISECONDS)

  // returns previously associated value, if any.
  def putToCacheIfAbsent(key: K, value: V): V = cache.asMap().putIfAbsent(key, value)
  def removeFromCache(key: K): V = cache.asMap().remove(key)
}

abstract class ExpirationRegistry[K <: Object, V <: Expirable](expirableMap: Option[ExpirableMap[K, V]] = None) {

  def insertOrRefreshEntry(key: K, value: V): Unit =
    expirableMap.foreach { _.putToCacheIfAbsent(key, value) }

  def removeEntry(key: K): Unit =
    expirableMap.foreach { _.removeFromCache(key) }
}
