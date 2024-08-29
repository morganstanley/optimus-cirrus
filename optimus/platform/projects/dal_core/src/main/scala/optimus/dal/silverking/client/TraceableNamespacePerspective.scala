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
package optimus.dal.silverking.client

import java.util.{Map => JMap}
import java.util.{Set => JSet}

import com.ms.silverking.cloud.dht.GetOptions
import com.ms.silverking.cloud.dht.InvalidationOptions
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions
import com.ms.silverking.cloud.dht.PutOptions
import com.ms.silverking.cloud.dht.RetrievalOptions
import com.ms.silverking.cloud.dht.WaitOptions
import com.ms.silverking.cloud.dht.client.AsyncInvalidation
import com.ms.silverking.cloud.dht.client.AsyncPut
import com.ms.silverking.cloud.dht.client.AsyncRetrieval
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval
import com.ms.silverking.cloud.dht.client.AsyncValueRetrieval
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective
import com.ms.silverking.cloud.dht.client.StoredValue
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective
import com.ms.silverking.cloud.dht.trace.TraceIDProvider
import optimus.dal.silverking.SkTraceIdProviderImpl
import optimus.dsi.trace.TraceId

trait TraceableAsyncOp[K, V] {
  def retrieveWithTrace(keys: JSet[_ <: K], retrievalOptions: RetrievalOptions, traceId: TraceId): AsyncRetrieval[K, V]
  def getWithTrace(keys: JSet[_ <: K], getOptions: GetOptions, traceId: TraceId): AsyncRetrieval[K, V]
  def getWithTrace(keys: JSet[_ <: K], traceId: TraceId): AsyncRetrieval[K, V]
  def getWithTrace(key: K, getOptions: GetOptions, traceId: TraceId): AsyncRetrieval[K, V]
  def getWithTrace(key: K, traceId: TraceId): AsyncRetrieval[K, V]

  def putWithTrace(values: JMap[_ <: K, _ <: V], putOptions: PutOptions, traceId: TraceId): AsyncPut[K]
  def putWithTrace(values: JMap[_ <: K, _ <: V], traceId: TraceId): AsyncPut[K]
  def putWithTrace(key: K, value: V, putOptions: PutOptions, traceId: TraceId): AsyncPut[K]
  def putWithTrace(key: K, value: V, traceId: TraceId): AsyncPut[K]

  def invalidateWithTrace(
      keys: JSet[_ <: K],
      invalidationOptions: InvalidationOptions,
      traceId: TraceId): AsyncInvalidation[K]
  def invalidateWithTrace(keys: JSet[_ <: K], traceId: TraceId): AsyncInvalidation[K]
  def invalidateWithTrace(key: K, invalidationOptions: InvalidationOptions, traceId: TraceId): AsyncInvalidation[K]
  def invalidateWithTrace(key: K, traceId: TraceId): AsyncInvalidation[K]

  def waitForWithTrace(keys: JSet[_ <: K], waitOptions: WaitOptions, traceId: TraceId): AsyncRetrieval[K, V]
  def waitForWithTrace(keys: JSet[_ <: K], traceId: TraceId): AsyncRetrieval[K, V]
  def waitForWithTrace(key: K, waitOptions: WaitOptions, traceId: TraceId): AsyncRetrieval[K, V]
  def waitForWithTrace(key: K, traceId: TraceId): AsyncRetrieval[K, V]
}

trait TraceableSyncOp[K, V] {
  def retrieveWithTrace(
      keys: JSet[_ <: K],
      retrievalOptions: RetrievalOptions,
      traceId: TraceId): JMap[K, _ <: StoredValue[V]]
  def retrieveWithTrace(key: K, retrievalOptions: RetrievalOptions, traceId: TraceId): StoredValue[V]
  def retrieveWithTrace(key: K, traceId: TraceId): StoredValue[V]
  def getWithTrace(keys: JSet[_ <: K], getOptions: GetOptions, traceId: TraceId): JMap[K, _ <: V]
  def getWithTrace(keys: JSet[_ <: K], traceId: TraceId): JMap[K, _ <: V]
  def getWithTrace(key: K, getOptions: GetOptions, traceId: TraceId): V
  def getWithTrace(key: K, traceId: TraceId): V

  def putWithTrace(values: JMap[_ <: K, _ <: V], putOptions: PutOptions, traceId: TraceId): Unit
  def putWithTrace(values: JMap[_ <: K, _ <: V], traceId: TraceId): Unit
  def putWithTrace(key: K, value: V, putOptions: PutOptions, traceId: TraceId): Unit
  def putWithTrace(key: K, value: V, traceId: TraceId): Unit

  def invalidateWithTrace(keys: JSet[_ <: K], invalidationOptions: InvalidationOptions, traceId: TraceId): Unit
  def invalidateWithTrace(keys: JSet[_ <: K], traceId: TraceId): Unit
  def invalidateWithTrace(key: K, invalidationOptions: InvalidationOptions, traceId: TraceId): Unit
  def invalidateWithTrace(key: K, traceId: TraceId): Unit

  def waitForWithTrace(keys: JSet[_ <: K], waitOptions: WaitOptions, traceId: TraceId): JMap[K, _ <: V]
  def waitForWithTrace(keys: JSet[_ <: K], traceId: TraceId): JMap[K, _ <: V]
  def waitForWithTrace(key: K, waitOptions: WaitOptions, traceId: TraceId): V
  def waitForWithTrace(key: K, traceId: TraceId): V
}

class TraceableAsyncNamespacePerspective[K, V] private[client] (
    private[client] val kernel: AsynchronousNamespacePerspective[K, V])
    extends TraceableAsyncOp[K, V] {

  def asVanilla: AsynchronousNamespacePerspective[K, V] = kernel

  private def getNspOptions: NamespacePerspectiveOptions[K, V] = kernel.getOptions
  private def mkTraceIdProvider(traceId: TraceId): TraceIDProvider = {
    new SkTraceIdProviderImpl(traceId)
  }

  // GET :: Base impl
  private def baseTraceRetrieve(
      keys: JSet[_ <: K],
      retrievalOptions: RetrievalOptions,
      traceIdProvider: TraceIDProvider): AsyncRetrieval[K, V] = {
    val hooked = retrievalOptions.traceIDProvider(traceIdProvider)
    kernel.retrieve(keys, hooked)
  }
  private def baseTraceGet(
      keys: JSet[_ <: K],
      getOptions: GetOptions,
      traceIdProvider: TraceIDProvider): AsyncValueRetrieval[K, V] = {
    val hooked = getOptions.traceIDProvider(traceIdProvider)
    kernel.get(keys, hooked)
  }
  private def baseTraceGet(
      key: K,
      getOptions: GetOptions,
      traceIdProvider: TraceIDProvider): AsyncSingleValueRetrieval[K, V] = {
    val hooked = getOptions.traceIDProvider(traceIdProvider)
    kernel.get(key, hooked)
  }
  // GET :: TraceableAsyncOp impl
  override def retrieveWithTrace(
      keys: JSet[_ <: K],
      retrievalOptions: RetrievalOptions,
      traceId: TraceId): AsyncRetrieval[K, V] = {
    baseTraceRetrieve(keys, retrievalOptions, mkTraceIdProvider(traceId))
  }
  override def getWithTrace(keys: JSet[_ <: K], getOptions: GetOptions, traceId: TraceId): AsyncValueRetrieval[K, V] = {
    baseTraceGet(keys, getOptions, mkTraceIdProvider(traceId))
  }
  override def getWithTrace(keys: JSet[_ <: K], traceId: TraceId): AsyncValueRetrieval[K, V] = {
    baseTraceGet(keys, getNspOptions.getDefaultGetOptions, mkTraceIdProvider(traceId))
  }
  override def getWithTrace(key: K, getOptions: GetOptions, traceId: TraceId): AsyncSingleValueRetrieval[K, V] = {
    baseTraceGet(key, getOptions, mkTraceIdProvider(traceId))
  }
  override def getWithTrace(key: K, traceId: TraceId): AsyncSingleValueRetrieval[K, V] = {
    baseTraceGet(key, getNspOptions.getDefaultGetOptions, mkTraceIdProvider(traceId))
  }

  // PUT :: Base impl
  private def baseTracePut(
      values: JMap[_ <: K, _ <: V],
      putOptions: PutOptions,
      traceIdProvider: TraceIDProvider): AsyncPut[K] = {
    val hooked = putOptions.traceIDProvider(traceIdProvider)
    kernel.put(values, hooked)
  }
  private def baseTracePut(key: K, value: V, putOptions: PutOptions, traceIdProvider: TraceIDProvider): AsyncPut[K] = {
    val hooked = putOptions.traceIDProvider(traceIdProvider)
    kernel.put(key, value, hooked)
  }
  // PUT :: TraceableAsyncOp impl
  override def putWithTrace(values: JMap[_ <: K, _ <: V], putOptions: PutOptions, traceId: TraceId): AsyncPut[K] = {
    baseTracePut(values, putOptions, mkTraceIdProvider(traceId))
  }
  override def putWithTrace(values: JMap[_ <: K, _ <: V], traceId: TraceId): AsyncPut[K] = {
    baseTracePut(values, getNspOptions.getDefaultPutOptions, mkTraceIdProvider(traceId))
  }
  override def putWithTrace(key: K, value: V, putOptions: PutOptions, traceId: TraceId): AsyncPut[K] = {
    baseTracePut(key, value, putOptions, mkTraceIdProvider(traceId))
  }
  override def putWithTrace(key: K, value: V, traceId: TraceId): AsyncPut[K] = {
    baseTracePut(key, value, getNspOptions.getDefaultPutOptions, mkTraceIdProvider(traceId))
  }

  // INVALIDATE :: Base impl
  private def baseTraceInvalidate(
      keys: JSet[_ <: K],
      invalidationOptions: InvalidationOptions,
      traceIdProvider: TraceIDProvider): AsyncInvalidation[K] = {
    val hooked = invalidationOptions.traceIDProvider(traceIdProvider)
    kernel.invalidate(keys, hooked)
  }
  private def baseTraceInvalidate(
      key: K,
      invalidationOptions: InvalidationOptions,
      traceIdProvider: TraceIDProvider): AsyncInvalidation[K] = {
    val hooked = invalidationOptions.traceIDProvider(traceIdProvider)
    kernel.invalidate(key, hooked)
  }
  // INVALIDATE :: TraceableAsyncOp impl
  override def invalidateWithTrace(
      keys: JSet[_ <: K],
      invalidationOptions: InvalidationOptions,
      traceId: TraceId): AsyncInvalidation[K] = {
    baseTraceInvalidate(keys, invalidationOptions, mkTraceIdProvider(traceId))
  }
  override def invalidateWithTrace(keys: JSet[_ <: K], traceId: TraceId): AsyncInvalidation[K] = {
    baseTraceInvalidate(keys, getNspOptions.getDefaultInvalidationOptions, mkTraceIdProvider(traceId))
  }
  override def invalidateWithTrace(
      key: K,
      invalidationOptions: InvalidationOptions,
      traceId: TraceId): AsyncInvalidation[K] = {
    baseTraceInvalidate(key, invalidationOptions, mkTraceIdProvider(traceId))
  }
  override def invalidateWithTrace(key: K, traceId: TraceId): AsyncInvalidation[K] = {
    baseTraceInvalidate(key, getNspOptions.getDefaultInvalidationOptions, mkTraceIdProvider(traceId))
  }

  // WAIT_FOR :: Base impl
  private def baseTraceWaitFor(
      keys: JSet[_ <: K],
      waitOptions: WaitOptions,
      traceIdProvider: TraceIDProvider): AsyncRetrieval[K, V] = {
    val hooked = waitOptions.traceIDProvider(traceIdProvider)
    kernel.waitFor(keys, hooked)
  }
  private def baseTraceWaitFor(
      key: K,
      waitOptions: WaitOptions,
      traceIdProvider: TraceIDProvider): AsyncRetrieval[K, V] = {
    val hooked = waitOptions.traceIDProvider(traceIdProvider)
    kernel.waitFor(key, hooked)
  }
  // WAIT_FOR :: TraceableAsyncOp impl
  override def waitForWithTrace(
      keys: JSet[_ <: K],
      waitOptions: WaitOptions,
      traceId: TraceId): AsyncRetrieval[K, V] = {
    baseTraceWaitFor(keys, waitOptions, mkTraceIdProvider(traceId))
  }
  override def waitForWithTrace(keys: JSet[_ <: K], traceId: TraceId): AsyncRetrieval[K, V] = {
    baseTraceWaitFor(keys, getNspOptions.getDefaultWaitOptions, mkTraceIdProvider(traceId))
  }
  override def waitForWithTrace(key: K, waitOptions: WaitOptions, traceId: TraceId): AsyncRetrieval[K, V] = {
    baseTraceWaitFor(key, waitOptions, mkTraceIdProvider(traceId))
  }
  override def waitForWithTrace(key: K, traceId: TraceId): AsyncRetrieval[K, V] = {
    baseTraceWaitFor(key, getNspOptions.getDefaultWaitOptions, mkTraceIdProvider(traceId))
  }
}

class TraceableSyncNamespacePerspective[K, V] private[client] (
    private[client] val kernel: SynchronousNamespacePerspective[K, V])
    extends TraceableSyncOp[K, V] {

  def asVanilla: SynchronousNamespacePerspective[K, V] = kernel

  private def getNspOptions: NamespacePerspectiveOptions[K, V] = kernel.getOptions
  private def mkTraceIdProvider(traceId: TraceId): TraceIDProvider = {
    new SkTraceIdProviderImpl(traceId)
  }

  // GET :: Base impl
  private def baseTraceRetrieve(
      keys: JSet[_ <: K],
      retrievalOptions: RetrievalOptions,
      traceIdProvider: TraceIDProvider): JMap[K, _ <: StoredValue[V]] = {
    val hook = retrievalOptions.traceIDProvider(traceIdProvider)
    kernel.retrieve(keys, hook)
  }
  private def baseTraceRetrieve(
      key: K,
      retrievalOptions: RetrievalOptions,
      traceIdProvider: TraceIDProvider): StoredValue[V] = {
    val hook = retrievalOptions.traceIDProvider(traceIdProvider)
    kernel.retrieve(key, hook)
  }
  private def baseTraceGet(
      keys: JSet[_ <: K],
      getOptions: GetOptions,
      traceIdProvider: TraceIDProvider): JMap[K, _ <: V] = {
    val hook = getOptions.traceIDProvider(traceIdProvider)
    kernel.get(keys, hook)
  }
  private def baseTraceGet(key: K, getOptions: GetOptions, traceIdProvider: TraceIDProvider): V = {
    val hook = getOptions.traceIDProvider(traceIdProvider)
    kernel.get(key, hook)
  }
  // GET :: TraceableSyncNamespacePerspective impl
  override def retrieveWithTrace(
      keys: JSet[_ <: K],
      retrievalOptions: RetrievalOptions,
      traceId: TraceId): JMap[K, _ <: StoredValue[V]] = {
    baseTraceRetrieve(keys, retrievalOptions, mkTraceIdProvider(traceId))
  }
  override def retrieveWithTrace(key: K, retrievalOptions: RetrievalOptions, traceId: TraceId): StoredValue[V] = {
    baseTraceRetrieve(key, retrievalOptions, mkTraceIdProvider(traceId))
  }
  override def retrieveWithTrace(key: K, traceId: TraceId): StoredValue[V] = {
    baseTraceRetrieve(key, getNspOptions.getDefaultGetOptions, mkTraceIdProvider(traceId))
  }
  override def getWithTrace(keys: JSet[_ <: K], getOptions: GetOptions, traceId: TraceId): JMap[K, _ <: V] = {
    baseTraceGet(keys, getOptions, mkTraceIdProvider(traceId))
  }
  override def getWithTrace(keys: JSet[_ <: K], traceId: TraceId): JMap[K, _ <: V] = {
    baseTraceGet(keys, getNspOptions.getDefaultGetOptions, mkTraceIdProvider(traceId))
  }
  override def getWithTrace(key: K, getOptions: GetOptions, traceId: TraceId): V = {
    baseTraceGet(key, getOptions, mkTraceIdProvider(traceId))
  }
  override def getWithTrace(key: K, traceId: TraceId): V = {
    baseTraceGet(key, getNspOptions.getDefaultGetOptions, mkTraceIdProvider(traceId))
  }

  // PUT :: Base impl
  private def baseTracePut(
      values: JMap[_ <: K, _ <: V],
      putOptions: PutOptions,
      traceIdProvider: TraceIDProvider): Unit = {
    val hook = putOptions.traceIDProvider(traceIdProvider)
    kernel.put(values, hook)
  }
  private def baseTracePut(key: K, value: V, putOptions: PutOptions, traceIdProvider: TraceIDProvider): Unit = {
    val hook = putOptions.traceIDProvider(traceIdProvider)
    kernel.put(key, value, hook)
  }
  // PUT :: TraceableSyncNamespacePerspective impl
  override def putWithTrace(values: JMap[_ <: K, _ <: V], putOptions: PutOptions, traceId: TraceId): Unit = {
    baseTracePut(values, putOptions, mkTraceIdProvider(traceId))
  }
  override def putWithTrace(values: JMap[_ <: K, _ <: V], traceId: TraceId): Unit = {
    baseTracePut(values, getNspOptions.getDefaultPutOptions, mkTraceIdProvider(traceId))
  }
  override def putWithTrace(key: K, value: V, putOptions: PutOptions, traceId: TraceId): Unit = {
    baseTracePut(key, value, putOptions, mkTraceIdProvider(traceId))
  }
  override def putWithTrace(key: K, value: V, traceId: TraceId): Unit = {
    baseTracePut(key, value, getNspOptions.getDefaultPutOptions, mkTraceIdProvider(traceId))
  }

  // INVALIDATE :: Base impl
  private def baseTraceInvalidate(
      keys: JSet[_ <: K],
      invalidationOptions: InvalidationOptions,
      traceIdProvider: TraceIDProvider): Unit = {
    val hook = invalidationOptions.traceIDProvider(traceIdProvider)
    kernel.invalidate(keys, hook)
  }
  private def baseTraceInvalidate(
      key: K,
      invalidationOptions: InvalidationOptions,
      traceIdProvider: TraceIDProvider): Unit = {
    val hook = invalidationOptions.traceIDProvider(traceIdProvider)
    kernel.invalidate(key, hook)
  }
  // INVALIDATE :: TraceableSyncNamespacePerspective impl
  override def invalidateWithTrace(
      keys: JSet[_ <: K],
      invalidationOptions: InvalidationOptions,
      traceId: TraceId): Unit = {
    baseTraceInvalidate(keys, invalidationOptions, mkTraceIdProvider(traceId))
  }
  override def invalidateWithTrace(keys: JSet[_ <: K], traceId: TraceId): Unit = {
    baseTraceInvalidate(keys, getNspOptions.getDefaultInvalidationOptions, mkTraceIdProvider(traceId))
  }
  override def invalidateWithTrace(key: K, invalidationOptions: InvalidationOptions, traceId: TraceId): Unit = {
    baseTraceInvalidate(key, invalidationOptions, mkTraceIdProvider(traceId))
  }
  override def invalidateWithTrace(key: K, traceId: TraceId): Unit = {
    baseTraceInvalidate(key, getNspOptions.getDefaultInvalidationOptions, mkTraceIdProvider(traceId))
  }

  // WAIT_FOR :: Base impl
  private def baseTraceWaitFor(
      keys: JSet[_ <: K],
      waitOptions: WaitOptions,
      traceIdProvider: TraceIDProvider): JMap[K, _ <: V] = {
    val hook = waitOptions.traceIDProvider(traceIdProvider)
    kernel.waitFor(keys, hook)
  }
  private def baseTraceWaitFor(key: K, waitOptions: WaitOptions, traceIdProvider: TraceIDProvider): V = {
    val hook = waitOptions.traceIDProvider(traceIdProvider)
    kernel.waitFor(key, hook)
  }
  // WAIT_FOR :: TraceableSyncNamespacePerspective impl
  override def waitForWithTrace(keys: JSet[_ <: K], waitOptions: WaitOptions, traceId: TraceId): JMap[K, _ <: V] = {
    baseTraceWaitFor(keys, waitOptions, mkTraceIdProvider(traceId))
  }
  override def waitForWithTrace(keys: JSet[_ <: K], traceId: TraceId): JMap[K, _ <: V] = {
    baseTraceWaitFor(keys, getNspOptions.getDefaultWaitOptions, mkTraceIdProvider(traceId))
  }
  override def waitForWithTrace(key: K, waitOptions: WaitOptions, traceId: TraceId): V = {
    baseTraceWaitFor(key, waitOptions, mkTraceIdProvider(traceId))
  }
  override def waitForWithTrace(key: K, traceId: TraceId): V = {
    baseTraceWaitFor(key, getNspOptions.getDefaultWaitOptions, mkTraceIdProvider(traceId))
  }
}
