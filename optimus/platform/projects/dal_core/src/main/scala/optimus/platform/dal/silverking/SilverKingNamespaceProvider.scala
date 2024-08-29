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
package optimus.platform.dal.silverking

import java.util.concurrent.atomic.AtomicReference

import com.ms.silverking.cloud.dht.GetOptions
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective
import com.ms.silverking.cloud.dht.client.BaseNamespacePerspective
import com.ms.silverking.cloud.dht.client.DHTSession
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective
import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.dal.config.DalZoneId

import scala.annotation.tailrec

private[optimus] trait SilverKingNamespaceProvider {
  def getSyncNamespace: SynchronousNamespacePerspective[Array[Byte], Array[Byte]]
  def getAsyncNamespace: AsynchronousNamespacePerspective[Array[Byte], Array[Byte]]
  def getDefaultGetOptions: GetOptions
  def getServerName: String
  def shutdown(): Unit
  def getNamespaceId: String
}

private[optimus] class NamespacePerspectiveProvider(connection: DHTSession, namespaceId: String) {
  import NamespacePerspectiveProvider.log

  private val _syncNsp = new AtomicReference[SynchronousNamespacePerspective[Array[Byte], Array[Byte]]]()
  private val _asyncNsp = new AtomicReference[AsynchronousNamespacePerspective[Array[Byte], Array[Byte]]]()

  @tailrec
  private def getOrCreateNsp[T <: BaseNamespacePerspective[Array[Byte], Array[Byte]]](
      ref: AtomicReference[T],
      createNsp: () => T): T = {
    val nsp = ref.get
    if (nsp ne null) {
      nsp
    } else {
      val newNsp = createNsp()
      if (ref.compareAndSet(null.asInstanceOf[T], newNsp)) {
        newNsp
      } else {
        newNsp.close()
        getOrCreateNsp(ref, createNsp)
      }
    }
  }

  def syncNsp: SynchronousNamespacePerspective[Array[Byte], Array[Byte]] = getOrCreateNsp(
    _syncNsp,
    () => {
      log.debug(s"Connecting to SilverKing namespace (sync): $namespaceId")
      connection
        .getNamespace(namespaceId)
        .openSyncPerspective[Array[Byte], Array[Byte]](classOf[Array[Byte]], classOf[Array[Byte]])
    }
  )

  def asyncNsp: AsynchronousNamespacePerspective[Array[Byte], Array[Byte]] = getOrCreateNsp(
    _asyncNsp,
    () => {
      log.debug(s"Connecting to SilverKing namespace (async): $namespaceId")
      connection
        .getNamespace(namespaceId)
        .openAsyncPerspective[Array[Byte], Array[Byte]](classOf[Array[Byte]], classOf[Array[Byte]])
    }
  )

  def close(): Unit = {
    val syncNsp = _syncNsp.getAndSet(null)
    val asyncNsp = _asyncNsp.getAndSet(null)
    if (syncNsp ne null) syncNsp.close()
    if (asyncNsp ne null) asyncNsp.close()
  }
}

object NamespacePerspectiveProvider {
  private val log = getLogger(NamespacePerspectiveProvider)
}
