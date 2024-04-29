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
package optimus.buildtool.cache.dht

import optimus.dht.client.api.kv.KVClient
import optimus.dht.client.api.kv.KVClient.BatchCallback
import optimus.dht.client.api.kv.KVClient.Callback
import optimus.dht.client.api.kv.KVKey
import optimus.dht.client.api.kv.KVLargeEntry
import optimus.dht.client.api.kv.KVLargeValue
import optimus.dht.client.api.transport.OperationDetails
import optimus.dht.common.api.Keyspace
import optimus.graph.Node
import optimus.graph.NodePromise
import optimus.platform.annotations.nodeSync
import optimus.platform.async
import optimus.platform.util.Log

import java.lang
import java.util
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class AsyncGraphLargeValueClient(kvClient: KVClient[KVKey]) extends Log {
  private def _contains(keyspace: Keyspace, keys: Set[KVKey], correlationName: String)(
      f: Try[Option[Set[KVKey]]] => Unit): Unit = {
    kvClient.batchContains(
      keyspace,
      keys.toList.asJava,
      correlationName,
      new BatchCallback[KVClient.KeyWithResult[lang.Boolean, KVKey], KVKey] {
        override def results(
            results: util.List[KVClient.KeyWithResult[lang.Boolean, KVKey]],
            opDetails: OperationDetails): Unit = {
          val ret = results.asScala.collect { case r if r.result() => r.key() }.toSet
          log.debug(s"DHT CHECK $correlationName: '$ret'")
          f(Success(Some(ret)))
        }
        override def errors(keys: util.List[KVKey], exception: Exception, opDetails: OperationDetails): Unit = {
          log.error(s"DHT CHECK $correlationName: ERROR: ${exception.getMessage}")
          f(Failure(exception))
        }
      }
    )
  }

  private def _putLarge(keyspace: Keyspace, entry: KVLargeEntry[KVKey], correlationName: String)(
      f: Try[Boolean] => Unit): Unit = {
    kvClient.putLargeValue(
      keyspace,
      entry,
      correlationName,
      new Callback[lang.Boolean] {
        override def result(result: lang.Boolean, opDetails: OperationDetails): Unit = {
          log.debug(s"DHT PUT $correlationName. Overwritten: ${result.booleanValue()}")
          f(Success(result.booleanValue()))
        }
        override def error(exception: Throwable, opDetails: OperationDetails): Unit = {
          log.error(s"DHT PUT $correlationName: ERROR: ${exception.getMessage}")
          f(Failure(exception))
        }
      }
    )
  }

  private def _getLarge(keyspace: Keyspace, key: KVKey, correlationName: String)(
      f: Try[Option[KVLargeValue]] => Unit): Unit = {
    kvClient.getLargeValue(
      keyspace,
      key,
      correlationName,
      new Callback[KVLargeEntry[KVKey]] {
        override def result(result: KVLargeEntry[KVKey], opDetails: OperationDetails): Unit = {
          val ret = Option(result.value)
          log.debug(s"DHT GET $correlationName. Found: '$ret'")
          f(Success(ret))
        }
        override def error(exception: Throwable, opDetails: OperationDetails): Unit = {
          log.error(s"DHT GET $correlationName: ERROR: ${exception.getMessage}")
          f(Failure(exception))
        }
      }
    )
  }

  @async def getLarge(keyspace: Keyspace, key: KVKey, correlationName: String): Option[KVLargeValue] = {
    impl(this._getLarge(keyspace, key, correlationName))
  }

  @async def putLarge(keyspace: Keyspace, entry: KVLargeEntry[KVKey], correlationName: String): Boolean = {
    impl(this._putLarge(keyspace, entry, correlationName))
  }

  @async def contains(keyspace: Keyspace, keys: Set[KVKey], correlationName: String): Option[Set[KVKey]] = {
    impl(this._contains(keyspace, keys, correlationName))
  }

  @nodeSync
  private def impl[A](f: (Try[A] => Unit) => Unit): A = impl$queued(f).get
  private def impl$queued[A](f: (Try[A] => Unit) => Unit): Node[A] = {
    val promise = NodePromise[A]()
    f(promise.complete)
    promise.node
  }

}
