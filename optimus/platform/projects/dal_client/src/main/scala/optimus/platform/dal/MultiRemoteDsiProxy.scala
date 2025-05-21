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

import optimus.dal.ssl.DalSSLConfig
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.session.EstablishSession
import optimus.platform._
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dal.messages.MessagesBrokerDsiProxy
import optimus.platform.dal.pubsub.PubSubClientRequest
import optimus.platform.dal.pubsub.PubSubClientResponse
import optimus.platform.dal.session.ClientSessionContext.SessionData
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal._
import optimus.platform.runtime.BrokerProviderResolver

import java.net.URI

abstract class AbstractMultiplexedDsiProxy extends ClientSideDSI {
  protected val leadWriter: ClientSideDSI
  protected def replicaOpt: Option[ClientSideDSI] = None
  protected val accOpt: Option[ClientSideDSI] = None
  protected val pubSubOpt: Option[ClientSideDSI] = None
  protected val messagesOpt: Option[ClientSideDSI] = None

  private[optimus] def getDSIClient: DSIClient =
    replicaOpt.map(_.getDSIClient).getOrElse(throw new IllegalStateException("No DSIClient available"))

  override def close(shutdown: Boolean): Unit = {
    leadWriter.close(shutdown)
    replicaOpt.foreach(_.close(shutdown))
    accOpt.foreach(_.close(shutdown))
    pubSubOpt.foreach(_.close(shutdown))
    messagesOpt.foreach(_.close(shutdown))
  }

  override def sessionData: SessionData = replicaOpt.getOrElse(leadWriter).sessionData

  override private[optimus] def setEstablishSession(command: => EstablishSession): Unit = {
    val establishSession = command
    leadWriter.setEstablishSession(establishSession)
    replicaOpt.foreach(_.setEstablishSession(establishSession))
    accOpt.foreach(_.setEstablishSession(establishSession))
    pubSubOpt.foreach(_.setEstablishSession(establishSession))
    messagesOpt.foreach(_.setEstablishSession(establishSession))
  }

  private[optimus] def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit = {
    replicaOpt.foreach(_.bindSessionFetcher(sessionFetcher))
    accOpt.foreach(_.bindSessionFetcher(sessionFetcher))
    pubSubOpt.foreach(_.bindSessionFetcher(sessionFetcher))
    messagesOpt.foreach(_.bindSessionFetcher(sessionFetcher))
  }

  @async override def executeReadOnlyCommands(cmds: Seq[ReadOnlyCommand]): Seq[Result] =
    replicaOpt.getOrElse(leadWriter).executeReadOnlyCommands(cmds)

  @async override def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result] =
    leadWriter.executeLeadWriterCommands(cmds)

  @async override def executeAccCommands(reads: Seq[ReadOnlyCommand]): Seq[Result] = {
    accOpt match {
      case Some(acc) => acc.executeReadOnlyCommands(reads)
      case None      => executeReadOnlyCommands(reads)
    }
  }

  override def executePubSubRequest(request: PubSubClientRequest): PubSubClientResponse =
    pubSubOpt.map(_.executePubSubRequest(request)).getOrElse(super.executePubSubRequest(request))

  @async override def executeMessagesCommands(cmds: Seq[MessagesCommand]): Seq[MessagesResult] =
    messagesOpt.map(_.executeMessagesCommands(cmds)).getOrElse(super.executeMessagesCommands(cmds))
}

final class MultiRemoteDsiProxy(
    val baseContext: Context,
    brokerProviderResolver: BrokerProviderResolver,
    leadBroker: String,
    replicaBroker: String,
    pubSubBroker: Option[String],
    val accBroker: Option[String],
    messagesBroker: Option[String],
    env: DalEnv,
    zone: DalZoneId,
    appId: DalAppId,
    override val partitionMap: PartitionMap,
    cmdLimit: Int,
    asyncConfig: DalAsyncConfig,
    shouldLoadBalance: Boolean,
    secureTransport: Boolean,
    val maxBrokersAllowedForConnectionFromURI: Int = 1)
    extends AbstractMultiplexedDsiProxy {
  override protected[optimus] val leadWriter =
    new LeaderRemoteDSIProxy(
      ClientBrokerContext(baseContext, leadBroker, zone, appId, Int.MaxValue, asyncConfig, brokerVirtualHostname(env)),
      brokerProviderResolver,
      DefaultPartition,
      partitionMap,
      secureTransport
    )
  private[optimus] val replica: ClientSideDSI = ReadBrokerDSIProxy(
    baseContext: Context,
    brokerProviderResolver,
    replicaBroker,
    partitionMap,
    zone,
    appId,
    cmdLimit,
    asyncConfig,
    env,
    shouldLoadBalance,
    secureTransport,
    maxBrokersAllowedForConnectionFromURI
  )

  override protected def replicaOpt: Option[ClientSideDSI] = Some(replica)

  override protected val accOpt: Option[ClientSideDSI] = accBroker
    .filter(_ != replicaBroker)
    .map(accUri =>
      new RandomRemoteDSIProxy(
        ClientBrokerContext(baseContext, accUri, zone, appId, cmdLimit, asyncConfig, brokerVirtualHostname(env)),
        brokerProviderResolver,
        partitionMap,
        false,
        false))
  override protected[optimus] val pubSubOpt: Option[ClientSideDSI with PubSubConnectionInternals] = pubSubBroker.map {
    pubSubUri =>
      new PubSubDsiProxy(
        ClientBrokerContext(baseContext, pubSubUri, zone, appId, Int.MaxValue, asyncConfig, brokerVirtualHostname(env)),
        brokerProviderResolver,
        partitionMap,
        secureTransport
      )
  }
  override protected[optimus] val messagesOpt: Option[ClientSideDSI] = messagesBroker.map { messagesUri =>
    new MessagesBrokerDsiProxy(
      ClientBrokerContext(baseContext, messagesUri, zone, appId, Int.MaxValue, asyncConfig, brokerVirtualHostname(env)),
      brokerProviderResolver,
      env)
  }

  // This must get called after session initialization is complete, otherwise it will fail.
  // There is no need to check/collect features from lead-writer as both should have the same
  // set of features. This is also safe in comparison to collecting features from lead-writer
  // because session initialization takes place with read-leader on app startup itself. Whereas
  // session initialization with lead-writer takes place only when the first write request is
  // sent.
  protected[optimus] override def serverFeatures(): SupportedFeatures = replica.serverFeatures()

  override def executePubSubRequest(request: PubSubClientRequest): PubSubClientResponse = {
    require(pubSubOpt.isDefined, "PubSubProxy should have already been created. Did you provide the pubsub arg?")
    super.executePubSubRequest(request)
  }
}

class MultiRemoteTcpDsiProxy(
    clientBrokerCtx: ClientBrokerContext,
    leadWriterUri: URI,
    replicaUri: Option[URI],
    pubSubUri: Option[URI],
    accUri: Option[URI],
    override val partitionMap: PartitionMap,
    val secureTransport: Boolean)
    extends AbstractMultiplexedDsiProxy {
  override val baseContext: Context = clientBrokerCtx.context
  override protected[optimus] val leadWriter =
    new RemoteDSIProxy(
      clientBrokerCtx.leadCtx,
      leadWriterUri.getHost,
      leadWriterUri.getPort,
      DalSSLConfig.forceKerberosedWriteBroker,
      partitionMap,
      secureTransport)
  override protected[optimus] val replicaOpt: Option[RemoteDSIProxy] = replicaUri map { uri =>
    new RemoteDSIProxy(clientBrokerCtx, uri.getHost, uri.getPort, false, partitionMap, secureTransport)
  }
  override protected[optimus] val pubSubOpt: Option[PubSubTcpDsiProxy] = pubSubUri map { uri =>
    new PubSubTcpDsiProxy(clientBrokerCtx.leadCtx, uri.getHost, uri.getPort, false, partitionMap, secureTransport)
  }
  override protected val accOpt: Option[RemoteDSIProxy] = accUri map { uri =>
    new RemoteDSIProxy(clientBrokerCtx, uri.getHost, uri.getPort, false, partitionMap, false)
  }

  // This must get called after session initialization is complete, otherwise it will fail.
  // There is no need to check/collect features from lead-writer as both should have the same
  // set of features. This is also safe in comparison to collecting features from lead-writer
  // because session initialization takes place with read-leader on app startup itself. Whereas
  // session initialization with lead-writer takes place only when the first write request is
  // sent.
  override protected[optimus] def serverFeatures(): SupportedFeatures = leadWriter.serverFeatures()
}

class MultiKerberizedRemoteTcpDsiProxy(
    clientBrokerCtx: ClientBrokerContext,
    leadWriterUri: URI,
    replicaUri: Option[URI],
    pubSubUri: Option[URI],
    accUri: Option[URI],
    override val partitionMap: PartitionMap,
    val secureTransport: Boolean)
    extends AbstractMultiplexedDsiProxy {
  override val baseContext: Context = clientBrokerCtx.context
  override protected val leadWriter =
    new RemoteDSIProxy(
      clientBrokerCtx.leadCtx,
      leadWriterUri.getHost,
      leadWriterUri.getPort,
      true,
      partitionMap,
      parameterizedSecureTransport(leadWriterUri))
  override protected val replicaOpt: Option[RemoteDSIProxy] = replicaUri map { uri =>
    new RemoteDSIProxy(clientBrokerCtx, uri.getHost, uri.getPort, true, partitionMap, parameterizedSecureTransport(uri))
  }
  override protected[optimus] val pubSubOpt: Option[PubSubTcpDsiProxy] = pubSubUri map { uri =>
    new PubSubTcpDsiProxy(
      clientBrokerCtx.leadCtx,
      uri.getHost,
      uri.getPort,
      true,
      partitionMap,
      parameterizedSecureTransport(uri))
  }
  override protected val accOpt: Option[RemoteDSIProxy] = accUri map { uri =>
    new RemoteDSIProxy(clientBrokerCtx, uri.getHost, uri.getPort, true, partitionMap, false)
  }

  // This must get called after session initialization is complete, otherwise it will fail.
  // There is no need to check/collect features from lead-writer as both should have the same
  // set of features. This is also safe in comparison to collecting features from lead-writer
  // because session initialization takes place with read-leader on app startup itself. Whereas
  // session initialization with lead-writer takes place only when the first write request is
  // sent.
  override protected[optimus] def serverFeatures(): SupportedFeatures = leadWriter.serverFeatures()
  protected def parameterizedSecureTransport(uri: URI): Boolean = secureTransport
}

final class MultiAwsRemoteDsiProxy(
    val baseContext: Context,
    leadBroker: String,
    replicaBroker: String,
    zone: DalZoneId,
    appId: DalAppId,
    override val partitionMap: PartitionMap,
    cmdLimit: Int,
    asyncConfig: DalAsyncConfig)
    extends ClientSideDSI {
  private[optimus] val leadWriter =
    new AwsDsiProxy(ClientBrokerContext(baseContext, leadBroker, zone, appId, Int.MaxValue, asyncConfig, None), false)
  private[optimus] val replica =
    new AwsDsiProxy(ClientBrokerContext(baseContext, replicaBroker, zone, appId, cmdLimit, asyncConfig, None), false)

  private[optimus] override def getDSIClient: DSIClient = replica.getDSIClient
  override def sessionData: SessionData = replica.sessionCtx.sessionData

  override def close(shutdown: Boolean): Unit = {
    leadWriter.close(shutdown)
    replica.close(shutdown)
  }

  private[optimus] override def setEstablishSession(command: => EstablishSession): Unit = {
    val establishSession = command
    leadWriter.setEstablishSession(establishSession)
    replica.setEstablishSession(establishSession)
  }

  private[optimus] override def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit = {
    leadWriter.bindSessionFetcher(sessionFetcher)
    replica.bindSessionFetcher(sessionFetcher)
  }

  // This must get called after session initialization is complete, otherwise it will fail.
  // There is no need to check/collect features from lead-writer as both should have the same
  // set of features. This is also safe in comparison to collecting features from lead-writer
  // because session initialization takes place with read-leader on app startup itself. Whereas
  // session initialization with lead-writer takes place only when the first write request is
  // sent.
  protected[optimus] override def serverFeatures() = replica.serverFeatures()

  @async override final def executeReadOnlyCommands(cmds: Seq[ReadOnlyCommand]): Seq[Result] =
    replica.executeReadOnlyCommands(cmds)

  @async override final def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result] =
    leadWriter.executeLeadWriterCommands(cmds)

  override def executePubSubRequest(request: PubSubClientRequest): PubSubClientResponse = {
    throw new RuntimeException("DALPubSub is not enabled in AWS environment!")
  }
}
