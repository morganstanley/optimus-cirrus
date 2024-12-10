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
import com.ms.silverking.cloud.dht.client.AsyncOperation
// import msjava.base.spring.lifecycle.BeanState
import msjava.slf4jutils.scalalog.getLogger
import optimus.dal.silverking.client.TraceableSkConverters
import optimus.dsi.trace.TraceId
import optimus.platform._
import optimus.platform._
import optimus.platform.dal.client.BatchedRequestSender
import optimus.platform.dal.client.PrcKeyProvider
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dal.config.SilverKingLookup
import optimus.platform.dal.silverking.SilverKingNamespaceProvider
import optimus.platform.dsi.bitemporal.Command
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DALRetryableActionException
import optimus.platform.dsi.bitemporal.proto.CommandAppNameTag
import optimus.platform.dsi.bitemporal.proto.CommandLocation
import optimus.platform.dsi.bitemporal.proto.Dsi.ClientAppIdentifierProto
import optimus.platform.dsi.bitemporal.proto.Dsi.ContextProto
import optimus.platform.dsi.bitemporal.proto.Dsi.ResultProto
import optimus.platform.dsi.bitemporal.proto.Prc.PrcSingleKeyResponseProto
import optimus.platform.dsi.bitemporal.proto.ResultSerializer
import optimus.platform.dsi.prc.cache.ClientCommandsPrcUserOptions
import optimus.platform.dsi.prc.cache.CommandsPrcUserOptions
import optimus.platform.dsi.prc.cache.DualReadCommandsPrcUserOptions
import optimus.platform.dsi.prc.cache.NonTemporalPrcKeyUserOpts
import optimus.platform.dsi.prc.cache.PrcUserOptsSerializer
import optimus.platform.dsi.prc.session.PrcClientSessionInfo
import optimus.platform.dsi.protobufutils.BatchContext
import optimus.platform.dsi.protobufutils.ReadClientRequest
import optimus.platform.dsi.protobufutils.WriteClientRequest

import scala.jdk.CollectionConverters._
import scala.util.Success
import scala.util.control.NonFatal

object PrcRequestSender {
  private val log = getLogger(PrcRequestSender)

  // N.b. it is not expected that clients set requestedUser, except for the broker (during dual read)
  // reads where the connection is not authenticated as the broker proid which set a requested user
  // will result in ErrorResults from PRC!
  def retrieve(
      namespaceProvider: SilverKingNamespaceProvider,
      asyncOpBatch: Map[Int, NonTemporalPrcKeyUserOpts],
      traceId: TraceId,
      prcUserOpts: CommandsPrcUserOptions,
      requestedUser: Option[String],
      zoneId: DalZoneId): (AsyncOperation, Map[ImmutableArray[Byte], Seq[Int]]) = {
    val serializedUserOpts = PrcUserOptsSerializer.serialize(prcUserOpts).toByteArray
    // we might have multiple requests for the same SK key with different user options (e.g. two selects of the same
    // entity reference at different temporal contexts). So we need to collect the cmdIndexes and keyUserOpts for
    // each SK key
    val cmdIndexesByKey: Map[ImmutableArray[Byte], Seq[Int]] = asyncOpBatch
      .groupBy { case (_, userOpts) => userOpts.key.getBackendKeyField }
      .map { case (key, values) => key -> values.keys.toSeq.sorted }
    val flatKeys = cmdIndexesByKey.keySet.map(_.rawArray).asJava

    val getOpts = getOptions(namespaceProvider, serializedUserOpts, requestedUser, zoneId)

    val vanillaAsyncNs = namespaceProvider.getAsyncNamespace
    val asyncOp = TraceableSkConverters
      .tryAsTraceable(vanillaAsyncNs)
      .map { traceableAsyncNs =>
        log.debug(
          s"Sending retrieve requests [$traceId] of [${flatKeys.size()}] key(s) to Sk server [${namespaceProvider.getServerName}]")
        traceableAsyncNs.getWithTrace(flatKeys, getOpts, traceId)
      }
      .getOrElse {
        // Currently PrcTrigger always expects TraceId is included, so if we hit this call-path, the PrcRequest will be redirected
        log.debug(
          s"request $traceId is not traced in server [${namespaceProvider.getServerName}] (server might haven't enabled trace feature)")
        vanillaAsyncNs.get(flatKeys, getOpts)
      }

    (asyncOp, cmdIndexesByKey)
  }

  def getOptions(
      namespaceProvider: SilverKingNamespaceProvider,
      serializedUserOpts: Array[Byte],
      requestedUser: Option[String],
      zoneId: DalZoneId): GetOptions = {
    val zoneToSend = if (zoneId == DalZoneId.unknown) DalZoneId.default else zoneId
    val base = namespaceProvider.getDefaultGetOptions
    val withUserOptsAndZid = base.userOptions(serializedUserOpts).zoneId(zoneToSend.underlying)
    requestedUser
      .map(r => withUserOptsAndZid.authorizationUser(r.getBytes()))
      .getOrElse(withUserOptsAndZid)
  }

  def commandToPrcKeyUserOpts(cmd: Command, keyProvider: PrcKeyProvider): NonTemporalPrcKeyUserOpts = {
    val (key, mappedCmd) =
      keyProvider.mkKey(cmd).getOrElse(throw new IllegalArgumentException(s"Command ineligible for PRC: $cmd"))
    NonTemporalPrcKeyUserOpts(key, mappedCmd)
  }

  type MessageType = (TraceId, CommandsPrcUserOptions, Int, DalZoneId)
}

class PrcRequestSender(
    client: DalPrcClient,
    messageReceiver: PrcMessageReceiver,
    keyProvider: PrcKeyProvider,
    skLoc: SilverKingLookup,
    baseContext: Context)
    extends BatchedRequestSender {
  import PrcRequestSender._

  // each message carries the PrcUserOptions and the seqId of the message
  override type MessageType = PrcRequestSender.MessageType

  // private[this] val state = new BeanState()
  // state.initializeIfNotInitialized()

  // NB we do not initialize this when instantiating the class as it has the side-effect of creating a connection to
  // an SK proxy. Rather we create the instance in start() below
  private var namespaceProvider: SilverKingNamespaceProvider = _
  protected val redirectionCache: PrcRedirectionCache = new PrcRedirectionCache

  override private[optimus] def start(): Unit = {
    try {
      /* state.startIfNotRunning(() => {
        namespaceProvider = mkNamespaceProvider()
      }) */
    } catch {
      case NonFatal(ex) =>
        log.error("Caught exception when creating PrcNamespaceProvider", ex)
        throw new DALRetryableActionException(ex.toString, Option(client))
    }
  }

  protected def mkNamespaceProvider(): PrcNamespaceProvider = {
    new PrcNamespaceProvider(skLoc, baseContext.contextType)
  }

  override private[optimus] def shutdown(): Unit = {
    /* state.destroyIfNotDestroyed(() => {
      if (namespaceProvider ne null) {
        namespaceProvider.shutdown()
        // assertConnected relies on the null check
        namespaceProvider = null
      }
      redirectionCache.clear()
    }) */
  }

  override private[optimus] def assertConnected(): Unit = synchronized {
    try {
      // state.throwIfNotRunning()
      require(namespaceProvider ne null, "Not connected to PRC.")
      // Can we get any info on whether the DHTSession connection is active?
    } catch {
      case NonFatal(ex) =>
        log.error("Caught exception when preparing to send the PRC request", ex)
        throw new DALRetryableActionException(ex.toString, Option(client))
    }
  }

  override private[platform] def buildMessage(
      seqId: Int,
      rc: BatchContext,
      contextProto: ContextProto,
      clientAppIdProto: ClientAppIdentifierProto,
      commandLocations: Seq[CommandLocation],
      commandTags: Seq[CommandAppNameTag]): MessageType = {
    val existingSession = rc.clientSessionContext.sessionData.existingSession
      .getOrElse(throw new IllegalArgumentException("Cannot use a prc client without an initialised session"))
    val prcClientSessionInfo = PrcClientSessionInfo(existingSession, baseContext)
    val traceId = TraceId(rc.requestUuid, rc.chainedId)
    val clientAppIdentifier = existingSession.applicationIdentifier
    val userOptss = rc.clientRequests.flatMap {
      case r: ReadClientRequest =>
        r.commands.map(PrcRequestSender.commandToPrcKeyUserOpts(_, keyProvider))
      case w: WriteClientRequest =>
        throw new IllegalArgumentException(s"Cannot execute write client request against PRC: $w")
    }
    val zoneId = clientAppIdentifier.zoneId
    (
      traceId,
      ClientCommandsPrcUserOptions(
        userOptss,
        prcClientSessionInfo,
        Some(existingSession.sessionId -> existingSession.establishingCommand),
        clientAppIdentifier),
      seqId,
      zoneId)
  }

  private[optimus] def mkAsyncListener(
      seqId: Int,
      cmdIndexesByKey: Map[ImmutableArray[Byte], Seq[Int]],
      asyncOpBatch: Map[Int, NonTemporalPrcKeyUserOpts]): PrcAsyncOperationListener = {
    new PrcAsyncOperationListener(
      seqId,
      messageReceiver,
      cmdIndexesByKey,
      asyncOpBatch,
      redirectionCache
    )
  }

  override private[optimus] def asyncSend(message: MessageType): Unit = ??? /* {
    val (traceId, prcUserOpts, seqId, zoneId) = message
    val asyncOpBatchBldr = Map.newBuilder[Int, NonTemporalPrcKeyUserOpts]
    val asyncOpBatchListBldr = Seq.newBuilder[NonTemporalPrcKeyUserOpts] // keeping track of these separately for order
    val alreadyCompletedResultsBldr = Seq.newBuilder[ResultProto]
    val alreadyCompletedCmdIndicesBldr = Seq.newBuilder[Int]

    prcUserOpts.prcKeyUserOptsList.zipWithIndex.foreach { case (userOpts, cmdIndex) =>
      val cachedRedirectionOpt = redirectionCache.get(userOpts)
      cachedRedirectionOpt
        .map { cachedRedirection =>
          alreadyCompletedResultsBldr += ResultSerializer.serialize(cachedRedirection)
          alreadyCompletedCmdIndicesBldr += cmdIndex
        }
        .getOrElse {
          asyncOpBatchBldr += (cmdIndex -> userOpts)
          asyncOpBatchListBldr += userOpts
        }
    }

    val asyncOpBatch = asyncOpBatchBldr.result()
    val asyncOpBatchList = asyncOpBatchListBldr.result()

    // if we have some results which we got locally, we send them straight to the messageReceiver
    val alreadyCompletedResults = alreadyCompletedResultsBldr.result()

    // if we have some results which weren't async-executed, we can send them straight to the messageReceiver
    if (alreadyCompletedResults.nonEmpty) {
      messageReceiver.messageCallback(
        DalPrcResponseMessage(
          seqId,
          Success(
            // this response is only partial if there are also async operations in-flight
            PrcResponseWrapper(
              isPartial = asyncOpBatch.nonEmpty,
              PrcSingleKeyResponseProto.newBuilder.addAllResults(alreadyCompletedResults.asJava).build(),
              alreadyCompletedCmdIndicesBldr.result()
            ))
        ))
    }

    // if we have some requests which need to be async sent to the server, we can now send them off
    if (asyncOpBatch.nonEmpty) {
      // if we have also completed results locally, we don't need to send the keys for those results to the server
      val filteredUserOptions =
        if (alreadyCompletedResults.isEmpty) prcUserOpts
        else {
          prcUserOpts match {
            case uo: ClientCommandsPrcUserOptions   => uo.copy(prcKeyUserOptsList = asyncOpBatchList)
            case uo: DualReadCommandsPrcUserOptions => uo.copy(prcKeyUserOptsList = asyncOpBatchList)
          }
        }
      val (asyncOp, cmdIndexesByKey) =
        PrcRequestSender.retrieve(namespaceProvider, asyncOpBatch, traceId, filteredUserOptions, None, zoneId)

      val listener = mkAsyncListener(seqId, cmdIndexesByKey, asyncOpBatch)
      asyncOp.addListener(listener)
      notifyAsyncRetrieveStarted(asyncOp, listener)
    }
  } */

  // test hooks
  protected def notifyAsyncRetrieveStarted(asyncOp: AsyncOperation, listener: PrcAsyncOperationListener): Unit = {}

}
