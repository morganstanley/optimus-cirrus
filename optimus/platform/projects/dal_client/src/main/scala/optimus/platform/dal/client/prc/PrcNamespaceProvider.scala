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
package optimus.platform.dal.client.prc

import com.ms.silverking.cloud.dht.GetOptions
import com.ms.silverking.cloud.dht.SessionOptions
import com.ms.silverking.cloud.dht.SessionPolicyOnDisconnect
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective
import com.ms.silverking.cloud.dht.client.DHTClient
import com.ms.silverking.cloud.dht.client.DHTSession
import com.ms.silverking.cloud.dht.client.SimpleSessionEstablishmentTimeoutController
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective
import com.ms.silverking.cloud.dht.client.impl.DHTSessionImpl
import msjava.slf4jutils.scalalog.getLogger
import optimus.dal.silverking.SkUtils
import optimus.dal.silverking.client.PrcNamespaceTemplate
import optimus.dal.silverking.client.PrcServiceConfig
import optimus.dal.silverking.client.SkClientKerberosUtils
import optimus.platform.dal.config.BrokerConfigSilverKingLookup
import optimus.platform.dal.config.DalServicesSilverKingLookup
import optimus.platform.dal.config.ExistingDhtSessionSilverKingLookup
import optimus.platform.dal.config.SilverKingLookup
import optimus.platform.dal.silverking.NamespacePerspectiveProvider
import optimus.platform.dal.silverking.SilverKingNamespaceProvider
import optimus.platform.dsi.bitemporal.ContextType

private[optimus] class PrcNamespaceProvider(
    skLoc: SilverKingLookup,
    baseContextType: ContextType,
    overrideHost: Option[String] = None)
    extends SilverKingNamespaceProvider {
  import PrcNamespaceProvider._

  // NB the call to get the plant config and namespace provider are context-independent so do not have to happen in
  // PrcNamespaceProvider (which is context-specific). However we do not expect typical clients to use multiple
  // contexts so this is not necessary for now.
  private val (connection, namespaceTemplate) = getConnectionAndNamespaceTemplate(skLoc, overrideHost)
  private val namespaceId = namespaceTemplate.namespaceForContextType(baseContextType)
  def getNamespaceId: String = namespaceId

  private val namespacePerspectives = new NamespacePerspectiveProvider(connection, namespaceId)

  override def getSyncNamespace: SynchronousNamespacePerspective[Array[Byte], Array[Byte]] =
    namespacePerspectives.syncNsp

  override def getAsyncNamespace: AsynchronousNamespacePerspective[Array[Byte], Array[Byte]] =
    namespacePerspectives.asyncNsp

  override def getDefaultGetOptions: GetOptions = getAsyncNamespace.getOptions.getDefaultGetOptions

  override def getServerName: String = skLoc.toString

  override def shutdown(): Unit = this.synchronized {
    log.info("Closing PRC connection")
    namespacePerspectives.close()
    connection.close()
  }
}

private object PrcNamespaceProvider {
  private val log = getLogger[PrcNamespaceProvider]
  private val prcSessionMaxAttempts: Int = Integer.getInteger("optimus.platform.dal.client.prc.sessionMaxAttempts", 5)
  private val prcSessionMaxRelativeTime: Int =
    Integer.getInteger("optimus.platform.dal.client.prc.sessionMaxRelativeTime", 3 * 60 * 1000)
  private val prcSessionAttemptRelativeTime: Int =
    Integer.getInteger("optimus.platform.dal.client.prc.sessionAttemptRelativeTime", 60 * 1000)

  private val sessionEstablishmentTimeoutController = new SimpleSessionEstablishmentTimeoutController(
    prcSessionMaxAttempts,
    prcSessionAttemptRelativeTime,
    prcSessionMaxRelativeTime)

  private def getConnectionAndNamespaceTemplate(
      skLoc: SilverKingLookup,
      overrideHost: Option[String] = None): (DHTSession, PrcNamespaceTemplate) =
    skLoc match {
      case d: DalServicesSilverKingLookup =>
        val (plantConfig, namespaceTemplate) = PrcServiceConfig.readZkConfig(d)
        val connectionInfo = plantConfig.getConnectionInfo(overrideHost = overrideHost)
        log.debug(s"Opening new PRC connection: $connectionInfo")
        if (connectionInfo.kerberized) SkClientKerberosUtils.enable()
        (
          SkUtils
            .createSkConnection(new DHTClient, plantConfig.getConnectionInfo(), sessionEstablishmentTimeoutController),
          namespaceTemplate)
      case e: ExistingDhtSessionSilverKingLookup =>
        val existingSession = e.getExistingSession.asInstanceOf[DHTSessionImpl]
        val newDhtSession =
          if (existingSession.isClosed) {
            log.warn(s"sk dht session is already closed, creating new one")
            val newSession = new DHTClient().openSession(
              new SessionOptions(existingSession.getDhtConfig)
                .sessionPolicyOnDisconnect(SessionPolicyOnDisconnect.CloseSession))
            e.setExistingSession(newSession)
            newSession
          } else
            e.getExistingSession
        (newDhtSession, e.namespaceTemplate)
      case _: BrokerConfigSilverKingLookup =>
        throw new IllegalArgumentException(s"Cannot get PRC connection details from brokerconfig")
    }
}
