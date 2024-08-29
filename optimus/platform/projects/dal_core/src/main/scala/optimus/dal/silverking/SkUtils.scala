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
package optimus.dal.silverking

import java.util.{Map => JMap}

import com.ms.silverking.cloud.dht.ConsistencyProtocol
import com.ms.silverking.cloud.dht.ForwardingMode
import com.ms.silverking.cloud.dht.LRURetentionPolicy
import com.ms.silverking.cloud.dht.LRWRetentionPolicy
import com.ms.silverking.cloud.dht.NamespaceOptions
import com.ms.silverking.cloud.dht.NamespaceServerSideCode
import com.ms.silverking.cloud.dht.NamespaceVersionMode
import com.ms.silverking.cloud.dht.SessionOptions
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration
import com.ms.silverking.cloud.dht.client.DHTClient
import com.ms.silverking.cloud.dht.client.DHTSession
import com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController
import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.dal.silverking.client.TraceableSkConverters
import optimus.dsi.trace.TraceId
import optimus.platform.dal.silverking.NamespacePerspectiveProvider

import scala.jdk.CollectionConverters._

object SkUtils {
  private val log: Logger = getLogger(this)

  val namespaceCreationTimeoutMillisProp = "optimus.dal.silverking.nsCreationTimeoutMills"
  private val placeholderTracedIdForUtil = "placeholderTracedIdForTriggerInitGuarantee"
  private val namespaceCreationTimeoutMillis = System.getProperty(namespaceCreationTimeoutMillisProp, "100000").toInt

  // means that everything that wants to get SLF4J logging out of SK client (or server) should initialize this object
  // as early as possible in its lifecycle

  def createSkConnection(
      client: DHTClient,
      connectionInfo: SilverKingConnectionInfo,
      timeoutController: SessionEstablishmentTimeoutController = SessionOptions.getDefaultTimeoutController
  ): DHTSession = {
    val sessionOptions = resolveConnInfo(connectionInfo, timeoutController).getSKSessionOptions
    log.info(
      s"Establishing a connection to SilverKing " +
        s"Proxy=[${sessionOptions.getPreferredServer}], " +
        s"DHTConfig=[${sessionOptions.getDHTConfig}], " +
        s"TimeoutSetup=[${sessionOptions.getTimeoutController}], " +
        s"SessionPolicyOnDisconnect=[${sessionOptions.getSessionPolicyOnDisconnect}]"
    )
    client.openSession(sessionOptions)
  }

  def createSkConnectionForHost(
      client: DHTClient,
      connectionInfo: SilverKingConnectionInfo,
      host: String): DHTSession = {
    val sessionOptions =
      resolveHcConnInfoForHost(connectionInfo, host).getSKSessionOptions
    log.info(
      s"Establishing a connection to SilverKing " +
        s"Proxy=[$host], " +
        s"DHTConfig=[${sessionOptions.getDHTConfig}], " +
        s"TimeoutSetup=[${sessionOptions.getTimeoutController}]")
    client.openSession(sessionOptions)
  }

  private[optimus] def resolveConnInfo(
      connectionInfo: SilverKingConnectionInfo,
      timeoutController: SessionEstablishmentTimeoutController
  ): DHTConnectionInfo = {
    val resolveEnvMap: JMap[String, String] =
      Map[String, String](
        ClientDHTConfiguration.nameVar -> connectionInfo.dhtName,
        ClientDHTConfiguration.portVar -> connectionInfo.fixedPort.toString,
        ClientDHTConfiguration.zkLocVar -> connectionInfo.zkLoc
      ).asJava
    DHTConnectionInfo(
      new SKGridConfiguration(connectionInfo.dhtName, resolveEnvMap),
      connectionInfo.hostList,
      timeoutController)
  }

  private def resolveHcConnInfoForHost(connectionInfo: SilverKingConnectionInfo, host: String): DHTConnectionInfo = {
    val resolveEnvMap: JMap[String, String] =
      Map[String, String](
        ClientDHTConfiguration.nameVar -> connectionInfo.dhtName,
        ClientDHTConfiguration.portVar -> connectionInfo.fixedPort.toString,
        ClientDHTConfiguration.zkLocVar -> connectionInfo.zkLoc
      ).asJava
    DHTConnectionInfo(new SKGridConfiguration(connectionInfo.dhtName, resolveEnvMap), Seq(host))
  }

  def closeSkConnection(connection: DHTSession): Unit = {
    log.info(s"Closing connection")
    connection.close()
  }

  def createMutableLooseNamespaceLru(
      connection: DHTSession,
      namespace: String,
      checkExistence: Boolean,
      lruCapacityBytes: Long,
      lruMaxVersion: Int,
      triggerClass: Class[_]): Unit = {
    val namespaceOptions = connection.getDefaultNamespaceOptions
      .versionMode(NamespaceVersionMode.SYSTEM_TIME_NANOS)
      .consistencyProtocol(ConsistencyProtocol.LOOSE)
      .valueRetentionPolicy(new LRURetentionPolicy(lruCapacityBytes, lruMaxVersion))
      .namespaceServerSideCode(NamespaceServerSideCode.singleTrigger(triggerClass))

    log.info(
      s"Creating a mutable(SYSTEM_TIME_NANOS mode) and loose-consistency namespace [$namespace] with LRU(capacity=${lruCapacityBytes}bytes,maxVer=$lruMaxVersion)")
    createNamespace(connection, namespace, namespaceOptions, checkExistence)
  }

  def createMutableLooseNamespaceLrw(
      connection: DHTSession,
      namespace: String,
      checkExistence: Boolean,
      lrwCapacityBytes: Long): Unit = {
    val namespaceOptions = connection.getDefaultNamespaceOptions
      .versionMode(NamespaceVersionMode.SYSTEM_TIME_NANOS)
      .consistencyProtocol(ConsistencyProtocol.LOOSE)
      .valueRetentionPolicy(new LRWRetentionPolicy(lrwCapacityBytes))

    log.info(
      s"Creating a mutable(SYSTEM_TIME_NANOS mode) and loose-consistency namespace [$namespace] with LRW(capacity=${lrwCapacityBytes}bytes)")
    createNamespace(connection, namespace, namespaceOptions, checkExistence)
  }

  def createMutableLooseNamespaceNoLru(
      connection: DHTSession,
      namespace: String,
      checkExistence: Boolean
  ): Unit = {
    val namespaceOptions = connection.getDefaultNamespaceOptions
      .versionMode(NamespaceVersionMode.SYSTEM_TIME_NANOS)
      .consistencyProtocol(ConsistencyProtocol.LOOSE)

    log.info(
      s"Creating a mutable(SYSTEM_TIME_NANOS mode) and loose-consistency namespace [$namespace], checkExistence=[$checkExistence]")
    createNamespace(connection, namespace, namespaceOptions, checkExistence)
  }

  def createNamespace(
      connection: DHTSession,
      namespace: String,
      nsOptions: NamespaceOptions,
      checkExistence: Boolean): Unit = {
    if (checkExistence && namespaceExists(connection, namespace)) {
      log.info(s"Skipping creation of namespace $namespace as it already exists.")
    } else {
      connection.createNamespace(namespace, nsOptions)
    }
  }

  def namespaceExists(session: DHTSession, namespace: String): Boolean =
    try {
      session.getNamespace(namespace) != null
    } catch {
      case _: NamespaceNotCreatedException => false
    }

  def removeNamespace(connection: DHTSession, namespace: String): Unit = {
    log.info(s"Deleting sync namespace [$namespace]")
    connection.deleteNamespace(namespace)
  }

  def withTriggerInitGuarantee(connection: DHTSession, namespace: String)(f: () => Unit): Unit = {
    runNspOperation(connection, namespace) { namespaceProvider =>
      f()
      // Simply creating the namespace isn't really enough -- we also want to ensure the server initializes it since
      // this will cause the server-side trigger to get loaded (which will in turn start the replication runner etc.)
      // As such we have to issue some actual query against this namespace. It doesn't matter if the query fails --
      // here we're trying to retrieve a key which clearly won't exist -- as we just need the namespace to have been
      // initialized, not to have actually served a valid query.
      val nsp = namespaceProvider.syncNsp
      val getOptions = nsp.getOptions.getDefaultGetOptions
        .opTimeoutController(new OpSizeBasedTimeoutController().constantTimeMillis(namespaceCreationTimeoutMillis))
        .forwardingMode(ForwardingMode.DO_NOT_FORWARD)
      // this get will fail but will result in the PRC trigger getting initialized by the SK server
      TraceableSkConverters
        .asTraceable(nsp)
        .getWithTrace(
          Array(1.toByte),
          getOptions,
          TraceId(placeholderTracedIdForUtil)
        )
    }
  }

  def runNspOperation(connection: DHTSession, namespace: String)(f: (NamespacePerspectiveProvider) => Unit): Unit = {
    val namespaceProvider = new NamespacePerspectiveProvider(connection, namespace)
    try {
      f(namespaceProvider)
    } finally {
      namespaceProvider.close()
      connection.close()
    }
  }

}
