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
package optimus.dal.ktcp

import msjava.slf4jutils.scalalog.getLogger
import optimus.config.OptimusConfigurationException
import optimus.config.RuntimeConfiguration
import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionBrokerLookup
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.partitioning.PartitionMapState
import optimus.graph.DiagnosticSettings
import optimus.platform.dal.ClientBrokerContext
import optimus.platform.dal.ClientSideDSI
import optimus.platform.dal.DSIClient
import optimus.platform.dal.DSIURIScheme
import optimus.platform.dal.MultiKerberizedRemoteTcpDsiProxy
import optimus.platform.dal.PartitionedDsiProxy
import optimus.platform.dal.RemoteDSIProxy
import optimus.platform.dal.RuntimeProperties
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalAsyncBatchingConfig
import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DSI
import optimus.platform.dsi.bitemporal.factory.DSIFactoryImpl
import optimus.platform.runtime.BrokerProviderResolver
import optimus.platform.runtime.OptimusCompositeLeaderElectorClient

import java.net.URI

class KtcpDSIFactoryImpl(config: RuntimeConfiguration, asyncConfigOpt: Option[DalAsyncConfig]) extends DSIFactoryImpl {
  thisFactory =>
  import KtcpDSIFactoryImpl.log

  private def isPartitionAware = PartitionMapState.getPartitionMapOpt.isDefined

  private val appId: DalAppId = DalAppId(
    config
      .getString(RuntimeProperties.DsiAppIdProperty)
      .getOrElse(
        throw new IllegalArgumentException(
          s"The application must provide an Application Id used for troubleshooting and capacity metrics. " +
            s"With OptimusAppCmdLine it can be supplied using the '-app' flag, or an 'APP_NAME' system property, " +
            s"or an 'APP_NAME' environment variable. If you are using a programmatic configuration you just set the " +
            s"config parameter to be the key defined at RuntimeProperties.DsiAppIdProperty.")))
  private val zone: DalZoneId = config
    .getString(RuntimeProperties.DsiZoneProperty)
    .map(DalZoneId(_))
    .getOrElse(RuntimeProperties.DsiZonePropertyDefaultValue)

  private val asyncConfig = asyncConfigOpt.getOrElse {
    val async: Boolean = asyncConfigOpt.map(_.async).getOrElse(SupportedFeatures.asyncClient)
    DalAsyncBatchingConfig(async)
  }

  // read from Zk first. This feature will be disabled if the Zk Value is set to +Inf
  private val readCmdLimit: Int =
    if (!asyncConfig.async) Int.MaxValue
    else {
      DiagnosticSettings.getIntProperty(
        RuntimeProperties.DsiReadCmdLimit,
        config
          .getInt(RuntimeProperties.DsiReadCmdLimit)
          .getOrElse(Int.MaxValue))
    }

  private val brokerProviderResolver =
    config
      .get(RuntimeProperties.DsiBrokerProviderResolver)
      .map(_.asInstanceOf[BrokerProviderResolver])
      .getOrElse(OptimusCompositeLeaderElectorClient.asBrokerProviderResolver)

  private def partitionMap: PartitionMap =
    config
      .get(PartitionMap.PartitionMapProperty)
      .getOrElse(
        throw new OptimusConfigurationException("Couldn't find the PartitionMap in the RuntimeConfiguration.")
      )
      .asInstanceOf[PartitionMap]

  private[optimus] def getPartitionBrokerLookup: PartitionBrokerLookup = PartitionBrokerLookup(config)

  override def uriScheme = DSIURIScheme.KTCP

  protected def secureTransport: Boolean = false

  protected def augment(
      context: ClientBrokerContext,
      authority: String,
      queryMap: => Map[String, String]): ClientBrokerContext = context
  protected def replicaFallback(replica: URI, queryMap: => Map[String, String]): URI = replica
  protected def writeFallback(write: Option[URI], queryMap: => Map[String, String]): Option[URI] = write
  protected def pubsubFallback(pubsub: Option[URI], queryMap: => Map[String, String]): Option[URI] = pubsub
  protected def parameterizedSecureTransport(secureTransport: Boolean, uri: URI): Boolean = secureTransport

  private def createNonPartitionedDSI(arg: URI, context: Context) = {
    log.info(
      "Partition map received from runtime configuration contains no partitions. " +
        "Creating non partitioned dsi proxy.")
    val queryMap = DSIURIScheme.getQueryMap(arg)
    val replica = replicaFallback(arg, queryMap)
    val pubSubUri =
      pubsubFallback(queryMap.get("pubsub").map(hostPort => new URI(s"$uriScheme://" + hostPort)), queryMap)
    val writer =
      writeFallback(queryMap.get("writer").map(hostPort => new URI(s"$uriScheme://" + hostPort)), queryMap)
    val accUri = queryMap.get("acc").map(hostPort => new URI(s"$uriScheme://" + hostPort))
    val ctx = augment(
      ClientBrokerContext(context, "", zone, appId, readCmdLimit, asyncConfig, None),
      arg.getAuthority,
      queryMap)
    if (pubSubUri.isEmpty && writer.isEmpty && accUri.isEmpty)
      new RemoteDSIProxy(ctx, replica.getHost, replica.getPort, true, partitionMap, secureTransport)
    else {
      new MultiKerberizedRemoteTcpDsiProxy(
        augment(
          ClientBrokerContext(context, "", zone, appId, readCmdLimit, asyncConfig, None),
          arg.getAuthority,
          queryMap),
        writer.getOrElse(replica),
        if (writer.nonEmpty) Some(replica) else None,
        pubSubUri,
        accUri,
        partitionMap,
        secureTransport
      ) {
        override protected def parameterizedSecureTransport(uri: URI): Boolean =
          thisFactory.parameterizedSecureTransport(this.secureTransport, uri)
      }
    }
  }

  private def createPartitionedDSI(arg: URI, context: Context) = {
    log.info(s"Partitions to be handled: ${partitionMap.allPartitions}. Creating PartitionedDsiProxy")
    val partitionToLookupMapping = getPartitionBrokerLookup
    val env = DalEnv(config.env)
    lazy val queryMap = DSIURIScheme.getQueryMap(arg)
    val ctx = augment(
      ClientBrokerContext(
        context,
        "",
        zone,
        appId,
        readCmdLimit,
        asyncConfig,
        ClientBrokerContext.brokerVirtualHostname(env)),
      arg.getAuthority,
      queryMap)

    new PartitionedDsiProxy(
      context,
      brokerProviderResolver,
      "<unused>",
      "<unused>",
      None,
      None,
      None,
      None,
      partitionMap,
      partitionToLookupMapping,
      zone,
      appId,
      readCmdLimit,
      asyncConfig,
      env,
      false,
      secureTransport
    ) {
      override def getOrCreateLeaderRemoteDsiProxy(broker: String, servingPartition: Partition): ClientSideDSI =
        new RemoteDSIProxy(ctx, arg.getHost, arg.getPort, true, this.partitionMap, secureTransport)
      override def getReplicaRemoteDsiProxy(broker: String, cmdLimit: Int, shouldLB: Boolean): DSIClient = {
        new RemoteDSIProxy(ctx, arg.getHost, arg.getPort, true, this.partitionMap, secureTransport)
      }
      protected[optimus] override lazy val replica: ClientSideDSI = {
        new RemoteDSIProxy(ctx, arg.getHost, arg.getPort, true, this.partitionMap, secureTransport)
      }
    }

  }
  override def createDSI(arg: URI, context: Context): ClientSideDSI = {
    if (isPartitionAware && !partitionMap.partitions.isEmpty) {
      createPartitionedDSI(arg, context)
    } else {
      createNonPartitionedDSI(arg, context)
    }
  }
}

object KtcpDSIFactoryImpl {
  private val log = getLogger(this)
}
