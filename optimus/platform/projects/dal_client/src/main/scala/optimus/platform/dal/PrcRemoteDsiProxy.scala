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
package optimus.platform.dal

import optimus.dsi.partitioning.PartitionMap
import optimus.graph.DiagnosticSettings
import optimus.platform.dal.client.PrcKeyProvider
import optimus.platform.dal.client.prc.DalPrcClient
import optimus.platform.dal.client.prc.DalPrcClientConfiguration
import optimus.platform.dal.client.prc.PrcEligibility
import optimus.platform.dal.config.SilverKingLookup
import optimus.platform.dal.session.ClientSessionContext.SessionData
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DefaultContext
import optimus.platform.dsi.bitemporal.NamedContext
import optimus.platform.dsi.bitemporal.ReadOnlyCommand
import optimus.platform.dsi.bitemporal.SharedContext
import optimus.platform.dsi.bitemporal.UniqueContext
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.protobufutils.DalBrokerClient
import optimus.platform.internal.SimpleStateHolder

object PrcRemoteDsiProxySettings {
  private val prcProxyEnabledProp = "optimus.platform.dal.client.prc.enabled"
}

final class PrcRemoteDsiProxySettings {
  import PrcRemoteDsiProxySettings._

  private def enabledDefault = DiagnosticSettings.getBoolProperty(prcProxyEnabledProp, true)
  private var enabled = DiagnosticSettings.getBoolProperty(prcProxyEnabledProp, enabledDefault)

  def isEnabled: Boolean = synchronized { enabled }
  def enable(): Unit = synchronized { enabled = true }
  def disable(): Unit = synchronized { enabled = false }
  def reset(): Unit = synchronized { enabled = enabledDefault }
}

object PrcRemoteDsiProxy extends SimpleStateHolder(() => new PrcRemoteDsiProxySettings) {
  def enabled: Boolean = getState.isEnabled

  def supportsContext(context: Context): Boolean = {
    context match {
      case DefaultContext | UniqueContext(_)  => true
      case SharedContext(_) | NamedContext(_) => false
    }
  }
}

class PrcRemoteDsiProxy(
    ctx: ClientBrokerContext,
    override val partitionMap: PartitionMap,
    skLoc: SilverKingLookup,
    keyProvider: PrcKeyProvider,
    replica: DSIClient)
    extends AbstractRemoteDSIProxy(ctx)
    with CommandProtoSerialization {
  override val sessionCtx = replica.sessionCtx

  // Since PRC uses replica's sessionCtx, this method always returns true
  override protected def canSessionCtxBeUsedWithNewSender: Boolean = true

  def eligibleForPrc(cmd: ReadOnlyCommand): Boolean = {
    val existingSession = sessionData match {
      // Session is in Initialized state
      case SessionData(Some(existingSession), None, _) => Some(existingSession)
      // Session is in PendingEstablish state so let's try to initialize
      case SessionData(None, Some(_), _) => Some(replica.getSession(true))
      // Session is in PendingReestablish state so let's try to initialize
      case SessionData(Some(_), Some(_), _) => Some(replica.getSession(true))
      // Session is Uninitialized
      case SessionData(None, None, _) => None
    }
    val eligible = existingSession.exists(s => PrcEligibility.isEligible(s, keyProvider, cmd))
    if (eligible) validatePartition(cmd)
    eligible
  }

  private def validatePartition(cmd: ReadOnlyCommand): Unit = {
    cmd.className().foreach(partitionMap.validate)
  }

  private def config: DalPrcClientConfiguration =
    new DalPrcClientConfiguration(SupportedFeatures.asyncClient, batchRetryManager)

  override protected def createBrokerClient(): DalBrokerClient =
    new DalPrcClient(ctx.clientContext, requestLimiter, config, skLoc, keyProvider)
}
