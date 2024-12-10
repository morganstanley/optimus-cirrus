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

import java.net.URI
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import msjava.msnet._
import msjava.msnet.internal.MSNetProtocolConnectionConfigurationSupport
// import msjava.msnet.utils.MSNetConfiguration
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.ChainedID
import optimus.platform.dal.ClientContext
import optimus.platform.dal.ClientRequestLimiter
import optimus.platform.dal.config.DalAsyncBatchingConfig
import optimus.platform.dal.config.DalAsyncConfig
import optimus.dal.ssl.DalSSLConfig
import optimus.platform.tls.SslConfigurationManager
import optimus.dsi.base.UriSchemeChecker
import optimus.dsi.partitioning.Partition
import optimus.platform.EvaluationContext
import optimus.platform.dal.client.BatchedRequestSenderFactory
import optimus.platform.dal.client.BatchingQueueProvider
import optimus.platform.dal.client.BatchingQueueProviderImpl
import optimus.platform.dal.config.DalEnv
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.connection.DalProtocolVersion
import optimus.platform.dsi.connection.DalProtocolVersionEstablisher
import optimus.platform.runtime.UriUtils

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object DALProtoClient {

  sealed trait ServerType
  case object DsiServer extends ServerType
  case object VersioningServer extends ServerType

  def getDalEnvOpt: Option[DalEnv] = {
    try {
      Option(EvaluationContext.scenarioStackOrNull)
        .flatMap(s => Option(s.env))
        .flatMap(e => Option(e.config))
        .flatMap(c => Option(c.runtimeConfig))
        .flatMap(rc => Option(rc.env))
        .filter(_.nonEmpty)
        .map(DalEnv(_))
    } catch {
      case NonFatal(ex) =>
        None
    }
  }

  def apply(
      uriString: String,
      clientContext: ClientContext,
      requestLimiter: ClientRequestLimiter,
      batchRetryManager: Option[BatchRetryManager] = None,
      config: Option[DALProtoClient.Configuration],
      writeServingPartitionOpt: Option[Partition] = None,
      secureTransport: Boolean,
      writeRequestLimiter: Option[SharedRequestLimiter]): DALProtoClient = {

    val actSecureTransport = secureTransport && DalSSLConfig.enabledZoneBasedClientSSL

    val (hostPort, kerberized) = {

      val uri = new URI(uriString)
      if (actSecureTransport)
        (
          s"${uri.getHost}:${Integer.parseInt(UriUtils.getQueryMap(uri)("sslPort"))}",
          UriSchemeChecker.isKerberized(uri))
      else
        (s"${uri.getHost}:${uri.getPort}", UriSchemeChecker.isKerberized(uri))
    }

    new DALProtoClient(
      hostPort,
      kerberized,
      clientContext,
      requestLimiter,
      actSecureTransport,
      writeRequestLimiter,
      batchRetryManager,
      config,
      writeServingPartitionOpt)
  }

  class Configuration(
      val asyncConfig: DalAsyncConfig = DalAsyncBatchingConfig.default,
      val name: String = "DALProtoClient",
      val serverType: ServerType = DsiServer,
      batchRetryManagerOpt: Option[BatchRetryManager] = None) {

    def largeBatchBreakdownCallback(): Unit = {
      // for testing purposes
    }

    def mkSenderFactory(
        client: DALProtoClient,
        messageCallback: (MSNetMessage, Option[DalProtocolVersion]) => Unit,
        shutdown: () => Unit,
        onConnect: () => Unit): BatchedRequestSenderFactory = mkSenderFactory(
      client.hostPort,
      client.kerberized,
      client.clientName,
      client,
      messageCallback,
      shutdown,
      onConnect,
      client.secureTransport)

    def mkSenderFactory(
        hostPort: String,
        kerberized: Boolean,
        clientName: String,
        client: DalBrokerClient,
        messageCallback: (MSNetMessage, Option[DalProtocolVersion]) => Unit,
        shutdown: () => Unit,
        onConnect: () => Unit,
        secureTransport: Boolean): BatchedRequestSenderFactory =
      () => {
        val configSupport = createConfigSupport(hostPort, kerberized, clientName, secureTransport)
        createTcpRequestSender(
          client,
          configSupport,
          secureTransport,
          protocolVersionOpt =>
            new DALClientMessageListener(toString, protocolVersionOpt, messageCallback, shutdown, onConnect)
        )
      }

    def createConfigSupport(
        hostPort: String,
        kerberized: Boolean,
        clientName: String,
        secureTransport: Boolean): MSNetProtocolConnectionConfigurationSupport = ??? /* {
      if (DalSSLConfig.enabledClientSSL) {
        MSNetConfiguration.setDefaultMSNetTCPSocketFactory(new MSNetSSLSocketFactory())
      }

      val c = new MSNetProtocolConnectionConfigurationSupport
      c.setHostPort(hostPort)
      c.setKerberos(kerberized)
      c.setLoop((new MSNetLoopThread(s"$clientName-loop")).startLoop())
      c.setRetryFlag(false) // never retry the same broker, because broker state may get changed.
      c.getPlainConnectionConfiguration.setConnectNotificationOnSyncConnect(true)
      callbackExecutor(clientName).foreach(c.setCallbackExecutor)

      if (secureTransport) {
        require(!DalSSLConfig.enabledClientSSL)

        val configuration = SslConfigurationManager.clientConfiguration(SslConfigurationManager.systemConfiguration())
        val mgr = SslConfigurationManager(configuration)
        val factory = mgr.openSslEngineFactory
        c.setSocketFactory(new MSNetSSLSocketFactory(factory, mgr.slicingBuffers))
      }

      c
    } */

    def createTcpRequestSender(
        client: DalBrokerClient,
        configSupport: MSNetProtocolConnectionConfigurationSupport,
        secureTransport: Boolean,
        msgListener: Option[DalProtocolVersion] => MSNetConnectionListenerAdapter,
        clientDalProtocolVersion: Option[DalProtocolVersion] = Some(
          DalProtocolVersionEstablisher.DefaultProtocolVersion)): TcpRequestSender = {
      new TcpRequestSender(client, this, configSupport, secureTransport, msgListener, clientDalProtocolVersion)
    }

    def createConnection(config: MSNetProtocolConnectionConfigurationSupport) = {
      new MSNetProtocolTCPConnection(config)
    }

    def timeoutCheckInterval: Long = TimeoutDetectorConfig.timeoutCheckInterval
    def maxWaitingTimeDALRequest: Int = TimeoutDetectorConfig.maxWaitingTimeDALRequest
    def timeoutDetectorConfig: TimeoutDetectorConfig =
      TimeoutDetectorConfig(timeoutCheckInterval, maxWaitingTimeDALRequest)

    val defaultConnectTimeout: Long = BatchedDalBrokerClient.defaultConnectTimeout

    // approximate value: will not start waiting for another item if exceeded (but will not interrupt a wait either)
    // TODO (OPTIMUS-11205): Waiting on msnet feature addition.
    val maxMessageSize: Int = ??? /* MSNetConfiguration.getDefaultMaxMessageSize() */

    // enableSyncOutOfOrderReception will take effect only in sync-mode( async=false )
    val enableSyncOutOfOrderReception = SupportedFeatures.enableSyncOutOfOrderReception

    def callbackExecutor(clientName: String): Option[ExecutorService] =
      Some(Executors.newSingleThreadExecutor(new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          val t = new Thread(r, s"$clientName-messageCallback")
          t.setDaemon(true)
          t
        }
      }))

    protected val batchingQueueProvider: BatchingQueueProvider =
      new BatchingQueueProviderImpl(asyncConfig, write = false, batchRetryManagerOpt)

    final lazy val batchingQueue: BatchingQueue[ClientRequest] with BatchingQueueShutdown[ClientRequest] =
      batchingQueueProvider.getBatchingQueue

    protected val batchingQueueProviderForWrites: BatchingQueueProvider =
      new BatchingQueueProviderImpl(asyncConfig, write = true, batchRetryManagerOpt)

    final lazy val batchingQueueForWrites: BatchingQueue[ClientRequest] with BatchingQueueShutdown[ClientRequest] =
      batchingQueueProviderForWrites.getBatchingQueue
  }

  private[DALProtoClient] val clientSerial = new AtomicInteger(0)
}

object InFlightRequestToken {
  private val log = getLogger(this)
}

class InFlightRequestToken {

  import InFlightRequestToken.log

  private val token = new Object

  private var completed = false
  private var uuid: Option[String] = None // only used for logging

  def waitForResponse = token.synchronized {
    log.debug("Entering wait")
    while (!completed) {
      token.wait(10000)
      if (!completed) log.info(s"Waiting for ${uuid.getOrElse("(no uuid yet)")} to return")
    }
    log.debug(s"${uuid.getOrElse("(no uuid yet)")} returned, leaving wait") // by this time uuid will have been set
  }

  def setUUID(uuid: String) = token.synchronized {
    this.uuid = Some(uuid)
  }

  def complete = token.synchronized {
    completed = true
    token.notifyAll()
    log.info(s"${uuid.getOrElse("(no uuid yet)")} completed") // by this time uuid will have been set
  }

}

class DALProtoClient(
    val hostPort: String,
    val kerberized: Boolean,
    clientCtx: ClientContext,
    reqLimiter: ClientRequestLimiter,
    val secureTransport: Boolean,
    writeRequestLimiter: Option[SharedRequestLimiter],
    batchRetryManager: Option[BatchRetryManager] = None,
    configOpt: Option[DALProtoClient.Configuration] = None,
    writeServingPartitionOpt: Option[Partition] = None
) extends BatchedDalBrokerClient(reqLimiter, clientCtx, batchRetryManager, writeServingPartitionOpt)
    with CommandProtoSerialization {
  outer =>
  private val config: DALProtoClient.Configuration =
    configOpt.getOrElse(new DALProtoClient.Configuration(clientCtx.asyncConfig))
  private[this] val serial = DALProtoClient.clientSerial.incrementAndGet
  override val toString = s"$clientName:$hostPort"
  override def clientName: String = s"${config.name}#$serial"

  override protected def createNewRequestSender: RequestSender =
    new RequestSender(
      this,
      config.mkSenderFactory(this, messageCallback, () => shutdown(None), onConnect _),
      clientCtx.context,
      ClientAppIdentifier(clientCtx.zoneId, clientCtx.appId)
    )

  final override protected def createNewRequestBatcher(requestSender: RequestSender): ClientRequestBatcher =
    createNewRequestBatcher(requestSender, write = false)

  final override protected def createOptionalNewRequestBatcherForWrites(
      requestSender: RequestSender): Some[ClientRequestBatcher] =
    Some(createNewRequestBatcher(requestSender, write = true))

  protected def createNewRequestBatcher(requestSender: RequestSender, write: Boolean): ClientRequestBatcher =
    new ClientRequestBatcher(
      requestSender,
      if (write) config.batchingQueueForWrites else config.batchingQueue,
      RequestBatcher.Configuration(clientName, config.asyncConfig.async, config.largeBatchBreakdownCallback _),
      if (write) writeRequestLimiter else None
    )

  override protected def createNewTimeoutDetector: TimeoutDetector =
    new TimeoutDetector(this, clientName, config.timeoutDetectorConfig)

  protected val messageReceiver =
    new DalProtoClientMessageReceiver(
      DALProtoClient.getDalEnvOpt.getOrElse(DalEnv.unknown),
      this,
      this.toString,
      config.asyncConfig.async || config.enableSyncOutOfOrderReception)

  override def connectionString: String = hostPort

  // facade retained for tests
  def messageCallback(message: MSNetMessage, protocolVersionOpt: Option[DalProtocolVersion]): Unit = {
    withRShutdownLock {
      if (isRunning) {
        messageReceiver.messageCallback(new DalBrokerResponseMessage(message, protocolVersionOpt))
      }
    }
  }

  def onConnect(): Unit = {}
}

object DALClientMessageListener {
  private val log = getLogger(this)
}

class DALClientMessageListener(
    protoclientDescription: String,
    protocolVersionOpt: Option[DalProtocolVersion],
    val onMessage: (MSNetMessage, Option[DalProtocolVersion]) => Unit,
    onDisconnect: () => Unit,
    onConnect: () => Unit)
    extends MSNetConnectionListenerAdapter {
  override def readCallback(name: MSNetID, message: MSNetMessage) = {
    DALClientMessageListener.log.trace("Received MSNetMessage")
    onMessage(message, protocolVersionOpt)
  }
  override def disconnectCallback(name: MSNetID) = {
    DALClientMessageListener.log.info(s"${protoclientDescription} disconnected")
    onDisconnect()
  }
  override def connectCallback(name: MSNetID) = {
    DALClientMessageListener.log.info(s"${protoclientDescription} connected")
    onConnect()
  }
}

trait BatchingQueueShutdown[T] extends BatchingQueue[T] {
  private val shutdownLock = new AtomicBoolean(false)
  def batchRetryManager: Option[BatchRetryManager] = None

  protected def preShutdown() = {} // called outside lock
  protected def preShutdownWithLock() = {} // called with lock
  protected def shutdownWithLock(): Unit // called with lock

  protected def offerOnShutdown(req: T): Unit

  // to guard against a client request getting lost (never completed) during shutdown procedure
  def shutdown(): Unit = {
    preShutdown()
    shutdownLock.synchronized {
      preShutdownWithLock()
      if (!shutdownLock.get) {
        val batchQueueSize = batchQueue.size
        val bufferSize = buffer.size
        log.info(
          s"Shutting down batching queue, this will set to failed $batchQueueSize in-flight requests from the queue and $bufferSize from the buffer")
        if (batchQueueSize > 0) log.debug(s"Queue: ${batchQueue.asScala.mkString(",")}")
        if (bufferSize > 0) log.debug(s"Buffer: ${buffer.mkString(",")}")

        shutdownWithLock()
        shutdownLock.set(true)
      }
    }
  }

  override def getNextBatch() = {

    try {

      require(!shutdownLock.get, "retrieving next batch after shutdown")

      super.getNextBatch()

    } catch {
      case ie: InterruptedException =>
        shutdown()
        throw ie
    }

  }

  override def offer(req: T): Boolean = {
    shutdownLock.synchronized {
      if (shutdownLock.get()) {
        offerOnShutdown(req)
        false
      } else {
        super.offer(req)
      }
    }
  }
}

trait ClientRequestBatchingQueueShutdown[T <: ClientRequest] extends BatchingQueueShutdown[T] {
  override protected def shutdownWithLock(): Unit = {
    @tailrec def failAllRequests(): Unit = {
      val batchesToSend = {
        val batches = if (batchQueue.size > 0) getNextNonBlockingBatch() else buffer.toVector
        buffer.clear()
        ClientRequestBatcher.filterNotCancelledRequests(batches)
      }

      if (batchesToSend.nonEmpty) {
        val it = batchesToSend.iterator
        it foreach { case (clientSessionContext, batch) =>
          val requestId = UUID.randomUUID
          val failedBatch = BatchContext(
            requestId.toString,
            batch.requests.headOption.map(_.nodeID.child).getOrElse(new ChainedID(requestId)),
            batch.requestType,
            clientSessionContext,
            batch.requests,
            None,
            None,
            batchRetryManager
          )
          failedBatch.completeAllWithException(None, new DALRetryableActionException("Connection shutdown", None))
        }
        failAllRequests()
      }
    }

    failAllRequests()
  }
  override protected def offerOnShutdown(req: T): Unit =
    completeRequestDueToShutdown(req.completable, true)
}
