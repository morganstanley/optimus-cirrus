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

import java.util.concurrent.atomic.AtomicInteger

import optimus.platform.dal.ClientContext
import optimus.platform.dal.ClientRequestLimiter
import optimus.platform.dal.config.DalAsyncBatchingConfig
import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dal.client.BatchedRequestSender
import optimus.platform.dal.client.BatchedRequestSenderFactory
import optimus.platform.dal.client.BatchingQueueProviderImpl
import optimus.platform.dal.client.PrcKeyProvider
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.SilverKingLookup
import optimus.platform.dsi.bitemporal.ClientAppIdentifier
import optimus.platform.dsi.protobufutils.BatchedDalBrokerClient
import optimus.platform.dsi.protobufutils.BatchRetryManager
import optimus.platform.dsi.protobufutils.BatchingQueue
import optimus.platform.dsi.protobufutils.BatchingQueueShutdown
import optimus.platform.dsi.protobufutils.ClientRequest
import optimus.platform.dsi.protobufutils.ClientRequestBatcher
import optimus.platform.dsi.protobufutils.DALProtoClient
import optimus.platform.dsi.protobufutils.RequestBatcher
import optimus.platform.dsi.protobufutils.RequestSender
import optimus.platform.dsi.protobufutils.TimeoutDetector
import optimus.platform.dsi.protobufutils.TimeoutDetectorConfig

object DalPrcClient {
  private val clientSerial = new AtomicInteger
  private val name: String = "DalPrcClient"
}

class DalPrcClient(
    clientCtx: ClientContext,
    reqLimiter: ClientRequestLimiter,
    config: DalPrcClientConfiguration,
    skLoc: SilverKingLookup,
    keyProvider: PrcKeyProvider)
    extends BatchedDalBrokerClient(
      reqLimiter,
      clientCtx,
      config.batchRetryManagerOpt,
      // PRC clients cannot be writers
      writeServingPartitionOpt = None)
    with BatchedRequestSenderFactory { outer =>

  private[this] val clientSerial = DalPrcClient.clientSerial.incrementAndGet
  private val timeoutDetectorConfig: TimeoutDetectorConfig = TimeoutDetectorConfig()
  protected val messageReceiver = {
    val env = DALProtoClient.getDalEnvOpt.getOrElse(DalEnv.unknown)
    new PrcMessageReceiver(env, this, connectionString)
  }

  override def createSender(): BatchedRequestSender =
    new PrcRequestSender(this, messageReceiver, keyProvider, skLoc, clientCtx.context)

  override protected def createNewRequestSender: RequestSender =
    new RequestSender(this, this, clientCtx.context, ClientAppIdentifier(clientCtx.zoneId, clientCtx.appId))

  override protected def createNewTimeoutDetector: TimeoutDetector =
    new TimeoutDetector(this, clientName, timeoutDetectorConfig)

  override protected def createNewRequestBatcher(requestSender: RequestSender): ClientRequestBatcher =
    new ClientRequestBatcher(
      requestSender,
      config.batchingQueue,
      RequestBatcher.Configuration(clientName, config.batchingConfig.async, () => ()),
      None)

  override def connectionString: String = skLoc.connectionString

  override def clientName: String = s"${DalPrcClient.name}#$clientSerial"

  override def toString: String = s"$clientName:$connectionString"
}

final class DalPrcClientConfiguration(async: Boolean, val batchRetryManagerOpt: Option[BatchRetryManager]) {
  val batchingConfig: DalAsyncConfig = DalAsyncBatchingConfig(async)
  protected val batchingQueueProvider =
    new BatchingQueueProviderImpl(batchingConfig, write = false, batchRetryManagerOpt)

  lazy val batchingQueue: BatchingQueue[ClientRequest] with BatchingQueueShutdown[ClientRequest] =
    batchingQueueProvider.getBatchingQueue
}
