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
package optimus.platform.dal.pubsub

import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.EvaluationContext
import optimus.platform.dal.DALEntityResolver
import optimus.platform.dal.MultiKerberizedRemoteTcpDsiProxy
import optimus.platform.dal.MultiRemoteDsiProxy
import optimus.platform.dal.MultiRemoteTcpDsiProxy
import optimus.platform.dal.PartitionedDsiProxy
import optimus.platform.dal.PubSubConnectionInternals

object PubSubConnectionHelper {

  private[this] val log = getLogger[PubSubConnectionHelper.type]

  def disablePubSubConnection(): Unit = {
    getPubSubDsiProxy.disableReading
  }

  def enablePubSubConnection(): Unit = {
    getPubSubDsiProxy.enableReading
  }

  def closeConnection(): Unit = {
    log.info(s"Simulating broker disconnect from ZK.")
    val dsi = EvaluationContext.env.entityResolver match {
      case ent: DALEntityResolver => ent.dsi
      case _ => throw new IllegalArgumentException("Don't expect other than DalEntityResolver here")
    }
    dsi match {
      case p: PartitionedDsiProxy              => p.pubSubProxyOpt.foreach(_.close(false))
      case r: MultiRemoteDsiProxy              => r.pubSubOpt.foreach(_.close(false))
      case t: MultiRemoteTcpDsiProxy           => t.pubSubOpt.foreach(_.close(false))
      case s: MultiKerberizedRemoteTcpDsiProxy => s.pubSubOpt.foreach(_.close(false))
      case _                                   => throw new IllegalArgumentException(s"Don't expect dsi of type: $dsi")
    }
  }

  def disconnectConnection(): Unit = {
    log.info(s"Simulating tcp disconnect from broker.")
    getPubSubDsiProxy.disconnectConnection
  }

  private def getPubSubDsiProxy: PubSubConnectionInternals = {
    val dsi = EvaluationContext.env.entityResolver match {
      case ent: DALEntityResolver => ent.dsi
      case _ => throw new IllegalArgumentException("Don't expect other than DalEntityResolver here")
    }
    val psProxyOpt = dsi match {
      case p: PartitionedDsiProxy              => p.pubSubProxyOpt
      case r: MultiRemoteDsiProxy              => r.pubSubOpt
      case t: MultiRemoteTcpDsiProxy           => t.pubSubOpt
      case s: MultiKerberizedRemoteTcpDsiProxy => s.pubSubOpt
      case _                                   => throw new IllegalArgumentException(s"Don't expect dsi of type: $dsi")
    }
    psProxyOpt.getOrElse(throw new IllegalArgumentException("Expected pubsub dsi proxy"))
  }
}
