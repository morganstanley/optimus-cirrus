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
package optimus.buildtool.cache.silverking

import optimus.buildtool.artifacts.CompilationMessage.Warning
import optimus.buildtool.cache.silverking.SilverKingStore._
import optimus.buildtool.cache.silverking.SilverKingStore.Connection._
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.trace.ConnectCache
import optimus.buildtool.trace.ObtTrace

import scala.util.control.NonFatal

object Connector {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}
private[silverking] class Connector(
    createOperations: () => SilverKingOperations,
    clusterType: ClusterType
) {
  import Connector.log

  private val clusterStr = SilverKingStore.clusterStr(clusterType)

  private var _connection: Option[Connection] = None
  private val lock = new Object

  protected def createConnection(retryCount: Int): Connection =
    try {
      ObtTrace.traceTask(RootScopeId, ConnectCache(clusterType), failureSeverity = Warning) {
        log.info(s"SilverKing $clusterType: Connecting...")
        val res = Connected(createOperations())
        log.info(s"SilverKing $clusterType: Connected")
        res
      }
    } catch {
      case NonFatal(t) =>
        val retryTime = now().plus(retryDelta(retryCount))
        val msg = s"Cache disabled due to connection failure: $t"
        log.warn(s"SilverKing $clusterType: $msg", t)
        Disconnected(msg, t, retryTime, retryCount + 1)
    }

  protected def now() = patch.MilliInstant.now()

  def connection(retryCount: Int = 0): Connection = lock.synchronized {
    _connection.getOrElse {
      val c = createConnection(retryCount)
      _connection = Some(c)
      c
    }
  }

  def connectionOption: Option[Connection] = lock.synchronized { _connection }

  def reconnect(retryCount: Int): Connection = lock.synchronized {
    disconnect()
    connection(retryCount)
  }

  def disconnect(): Unit = lock.synchronized {
    connectionOption match {
      case Some(Connected(c)) => c.close()
      case _                  => // do nothing
    }
    _connection = None
  }
}
