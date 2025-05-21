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

import optimus.platform.dal.config.DalAsyncConfig
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.PartitionMap
import optimus.platform._
import optimus.platform.dal.session.ClientSessionContext.SessionData
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dsi.bitemporal._
import optimus.dsi.session.EstablishSession
import optimus.platform.runtime.BrokerProviderResolver

final class DuplexRemoteBalancingDSIProxy(
    val baseContext: Context,
    brokerProviderResolver: BrokerProviderResolver,
    leadBroker: String,
    zone: DalZoneId,
    appId: DalAppId,
    override val partitionMap: PartitionMap,
    cmdLimit: Int,
    asyncConfig: DalAsyncConfig,
    val secureTransport: Boolean)
    extends ClientSideDSI {
  private val brokerContext =
    ClientBrokerContext(baseContext, leadBroker, zone, appId, cmdLimit, asyncConfig, None)
  private[optimus] val writeLeader =
    new LeaderRemoteDSIProxy(
      brokerContext.leadCtx,
      brokerProviderResolver,
      DefaultPartition,
      partitionMap,
      secureTransport)
  // Set load balancer as false as using the same as lead writer region
  private[optimus] val readLeader =
    new ReadRemoteDSIProxy(brokerContext, brokerProviderResolver, partitionMap, false, secureTransport)
  override def sessionData: SessionData = readLeader.sessionCtx.sessionData

  private[optimus] def getDSIClient: DSIClient = readLeader

  override def close(shutdown: Boolean): Unit = {
    writeLeader.close(shutdown)
    readLeader.close(shutdown)
  }

  private[optimus] override def setEstablishSession(command: => EstablishSession): Unit = {
    val establishSession = command
    writeLeader.setEstablishSession(establishSession)
    readLeader.setEstablishSession(establishSession)
  }
  private[optimus] override def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit = {
    writeLeader.bindSessionFetcher(sessionFetcher)
    readLeader.bindSessionFetcher(sessionFetcher)
  }

  // This must get called after session initialization is complete, otherwise it will fail.
  // There is no need to check/collect features from lead-writer as both should have the same
  // set of features. This is also safe in comparison to collecting features from lead-writer
  // because session initialization takes place with read-primary on app startup itself. Whereas
  // session initialization with lead-writer takes place only when the first write request is
  // sent.
  override protected[optimus] def serverFeatures() = readLeader.serverFeatures()

  @async override final def executeReadOnlyCommands(cmds: Seq[ReadOnlyCommand]): Seq[Result] =
    readLeader.executeReadOnlyCommands(cmds)

  @async override final def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result] =
    writeLeader.executeLeadWriterCommands(cmds)
}
