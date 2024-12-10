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
package optimus.platform.dsi.protobufutils

import java.lang.{Long => javaLong}
import java.util.concurrent.TimeUnit

import com.google.common.cache._
import optimus.platform.dal.config.DalAsyncBatchingConfig
import optimus.core.CoreAPI
import optimus.graph.DiagnosticSettings
import optimus.platform._
import optimus.platform.dal.ClientBrokerContext
import optimus.platform.dal.ClientRequestLimiter
import optimus.platform._
import optimus.platform.dal.session.ClientSessionContext
import optimus.platform.dsi.Response
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi.DSIRequestProto
import optimus.platform.dsi.protobufutils.DALProtoClient.{Configuration, VersioningServer}
import optimus.platform.dsi.versioning.PathHashT
import optimus.platform.dsi.versioning.VersioningKey
import optimus.platform.dsi.versioning.VersioningRedirectionInfo
// import optimus.platform.runtime.OptimusVersioningZkClient

import scala.annotation.tailrec
import scala.util.Random

object VersioningServerManager extends CoreAPI {
  val OperationRetryAttempts = Integer.getInteger("optimus.dsi.versioning.retryAttempts", 5)
  val OperationRetryBackoffTime = javaLong.getLong("optimus.dsi.versioning.retryBackoffTimeMs", 1000L)
  val MaxWaitingTimeVersioningRequest = Integer.getInteger("optimus.dsi.versioning.maxWaitingTime", Integer.MAX_VALUE)
  val DisableBatchRetry = DiagnosticSettings.getBoolProperty("optimus.dsi.versioning.disableBatchRetry", false)
  val ProtoClientCacheExpiry = javaLong.getLong("optimus.dsi.versioning.protoClientCacheExpiry", 30L)
  val ProtoClientCacheSize = javaLong.getLong("optimus.dsi.versioning.protoClientCacheSize", 10L)

  private def collate(results: Seq[Result]): Seq[Command] = {
    results flatMap {
      case validResult: VersioningValidResult => validResult.versionedCommands
      case errorResult: VersioningErrorResult => throw new RuntimeException(errorResult.error)
      case _                                  => Seq.empty[Command]
    }
  }

  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}

class VersioningServerManager(
    brokerContext: ClientBrokerContext,
    retryAttempts: Int = VersioningServerManager.OperationRetryAttempts,
    retryBackoffTime: Long = VersioningServerManager.OperationRetryBackoffTime,
    maxWaitingTime: Long = VersioningServerManager.MaxWaitingTimeVersioningRequest.longValue,
    disableBatchRetry: Boolean = VersioningServerManager.DisableBatchRetry) { outer =>

  import VersioningServerManager._

  protected val batchRetryManager: Option[BatchRetryManager] =
    if (disableBatchRetry) None else Some(new VersioningDsiClientBatchRetryManager(retryAttempts, retryBackoffTime))

  private final object RemovalListener extends RemovalListener[(ClientSessionContext, PathHashT), DalBrokerClient] {
    def onRemoval(notification: RemovalNotification[(ClientSessionContext, PathHashT), DalBrokerClient]) = {
      notification.getValue.shutdown(None)
    }
  }

  private[this] val protoClientCache = {
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(ProtoClientCacheExpiry, TimeUnit.MINUTES)
      .maximumSize(ProtoClientCacheSize)
      .concurrencyLevel(1)
      .removalListener(RemovalListener)
      .build(new CacheLoader[(ClientSessionContext, PathHashT), DalBrokerClient] {
        override def load(key: (ClientSessionContext, PathHashT)) = ??? /* {
          val (sessionCtx, classpathHash) = key
          val zkClient = OptimusVersioningZkClient.getClient(brokerContext.broker)
          // TODO (OPTIMUS-14722): support server-side versioning for non-default DAL contexts
          val versioningKey = VersioningKey(classpathHash, brokerContext.context)
          val connectionString = zkClient.getVersioningServer(versioningKey)
          val config = new Configuration(DalAsyncBatchingConfig.default, "VersioningProtoClient", VersioningServer)
          val protoClient = DALProtoClient(
            connectionString,
            brokerContext.clientContext,
            ClientRequestLimiter.unlimited,
            batchRetryManager,
            Some(config),
            None,
            false,
            SharedRequestLimiter.write)
          protoClient.start()
          protoClient
        } */
      })
  }

  private def closeProtoClients(sessionCtx: ClientSessionContext, redirectionInfo: VersioningRedirectionInfo) = {
    redirectionInfo.classpathHashes map { hash =>
      protoClientCache.invalidate((sessionCtx, hash))
    }
  }

  private def getOrCreateProtoClients(
      sessionCtx: ClientSessionContext,
      redirectionInfo: VersioningRedirectionInfo): Set[DalBrokerClient] = {
    redirectionInfo.classpathHashes map { hash =>
      protoClientCache.get((sessionCtx, hash))
    }
  }

  private def getOrCreateSingleProtoClient(
      sessionCtx: ClientSessionContext,
      redirectionInfo: VersioningRedirectionInfo): DalBrokerClient = {
    val protoClients = getOrCreateProtoClients(sessionCtx, redirectionInfo)
    if (protoClients.isEmpty)
      throw new InvalidRedirectionInfoException
    // TODO (OPTIMUS-13380): we'll need to support scattering the request among multiple versioning servers, one for each classpath hash
    protoClients.head
  }

  @async def version(
      sessionCtx: ClientSessionContext,
      cmds: Seq[Command],
      requestType: DSIRequestProto.Type,
      redirectionInfo: VersioningRedirectionInfo): Seq[Command] = {
    var attempts = retryAttempts
    var done = false
    var response: Response = null

    val randonmNum = new Random(System.currentTimeMillis)
    var backoff = retryBackoffTime + math.abs(randonmNum.nextLong() % retryBackoffTime)
    var hasRetried = false
    var results: Seq[Command] = null

    while (!done) {
      val res = asyncResult {
        val currentSender = getOrCreateSingleProtoClient(sessionCtx, redirectionInfo)
        response = currentSender.request(cmds, requestType, sessionCtx, Some(redirectionInfo))
        // TODO (OPTIMUS-13380): we'll need to support gathering results from multiple versioning servers
        results = VersioningServerManager.collate(response.results)

        if (hasRetried) DALEventMulticaster.dalVersioningRecoveryCallback(createDALRecoveryEvent(response.client))
        done = true
      }

      if (res.hasException) {
        def handleResponseException(ex: Exception): Unit = {
          if (shouldRetry(ex, requestType)) {
            if (attempts > 0) {
              log.warn(s"Versioning client retry, attempts left = ${attempts}, retrying in: ${backoff}ms")
              attempts -= 1
              DALEventMulticaster.dalVersioningRetryCallback(DALVersioningRetryEvent(ex))
              closeProtoClients(sessionCtx, redirectionInfo)
              // OptimusVersioningZkClient.reset()
              delay(backoff)
              backoff *= 2
              hasRetried = true
            } else {
              log.warn("Out of retries", ex)
              throw ex
            }
          } else {
            log.warn("This exception is not retryable", ex)
            throw ex
          }
        }
        res.exception match {
          case ex: DALNonRetryableActionException =>
            // we check for cause first, in case this was wrapped by BatchRetryManager
            val t = Option(ex.getCause) getOrElse ex
            throw t
          case ex: Exception => handleResponseException(ex)
          case t             => throw t
        }
      }

    }

    require(results ne null)
    results
  }

  def shutdown() = {
    protoClientCache.invalidateAll
    batchRetryManager foreach (_.shutdown())
  }

  protected def createDALRecoveryEvent(client: Option[DalBrokerClient]): DALVersioningRecoveryEvent =
    DALVersioningRecoveryEvent(s"Connected to ${client.map(_.connectionString).getOrElse("<unknown>")}")

  /* @tailrec */ private[this] def shouldRetry(t: Throwable, requestType: DSIRequestProto.Type): Boolean = ??? /* {
    val isRetryable = (requestType == DSIRequestProto.Type.WRITE && t.isInstanceOf[NoVersioningServersException])
    // has to be written like this so the recursive call is in tail position
    if (isRetryable)
      true
    else
      (t ne null) && shouldRetry(t.getCause, requestType)

  } */

  private class VersioningDsiClientBatchRetryManager(
      override val maxRetryAttempts: Int,
      override val retryBackoffTime: Long)
      extends BatchRetryManager {
    override protected def shouldRetry(t: Throwable, requestType: DSIRequestProto.Type): Boolean =
      outer.shouldRetry(t, requestType)
    override protected def sendRetryBatch(newBatch: BatchContext): Unit = {
      val currentSender =
        outer.getOrCreateSingleProtoClient(newBatch.clientSessionContext, newBatch.redirectionInfo.get)
      currentSender.sendBatch(newBatch)
    }
    override def raiseDALRetryEvent(ex: Exception): Unit =
      DALEventMulticaster.dalVersioningRetryCallback(DALVersioningRetryEvent(ex))
    override def raiseDALRecoveryEvent(client: Option[DalBrokerClient]): Unit =
      DALEventMulticaster.dalVersioningRecoveryCallback(outer.createDALRecoveryEvent(client))
    override protected def close(client: Option[DalBrokerClient], batch: BatchContext): Unit = {
      outer.closeProtoClients(batch.clientSessionContext, batch.redirectionInfo.get)
      // OptimusVersioningZkClient.reset()
    }
  }
}
