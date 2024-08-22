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

import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.google.inject.Key
import com.google.inject.TypeLiteral
import com.google.inject.multibindings.MapBinder
import com.google.inject.multibindings.OptionalBinder
import msjava.slf4jutils.scalalog._
import optimus.config.MissingConfigPropertyException
import optimus.config.OptimusConfigurationException
import optimus.config.RuntimeConfiguration
import optimus.dsi.partitioning.DefaultPartition
import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionBrokerLookup
import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.partitioning.PartitionMapState
import optimus.graph.DiagnosticSettings
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalAsyncBatchingConfig
import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalServicesSilverKingLookup
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DSI
import optimus.platform.dsi.bitemporal.factory.DSIFactoryImpl
import optimus.platform.dsi.bitemporal.factory.DsiFactoryModuleProvider
import optimus.platform.runtime.BrokerProviderResolver
import optimus.platform.runtime.OptimusCompositeLeaderElectorClient

import java.net.URI
import java.util.ServiceLoader
import java.util.{Map => JMap}
import javax.inject.Provider
import scala.jdk.CollectionConverters._
import scala.util.Try

object DSIFactory {
  private val log = getLogger(this)
  private val currentUser = System.getProperty("user.name")
}

class DSIFactory(config: RuntimeConfiguration, asyncConfigOpt: Option[DalAsyncConfig] = None) {
  outer =>
  import DSIFactory.log
  import DSIURIScheme.getQueryMap

  protected def currentUser: String = DSIFactory.currentUser
  private def isPartitionAware = PartitionMapState.getPartitionMapOpt.isDefined

  val appId: DalAppId = DalAppId(
    config
      .getString(RuntimeProperties.DsiAppIdProperty)
      .getOrElse(
        throw new IllegalArgumentException(
          s"The application must provide an Application Id used for troubleshooting and capacity metrics. " +
            s"With OptimusAppCmdLine it can be supplied using the '-app' flag, or an 'APP_NAME' system property, " +
            s"or an 'APP_NAME' environment variable. If you are using a programmatic configuration you just set the " +
            s"config parameter to be the key defined at RuntimeProperties.DsiAppIdProperty.")))
  val zone: DalZoneId = config
    .getString(RuntimeProperties.DsiZoneProperty)
    .map(DalZoneId(_))
    .getOrElse(RuntimeProperties.DsiZonePropertyDefaultValue)

  val asyncConfig = asyncConfigOpt.getOrElse {
    val async: Boolean = asyncConfigOpt.map(_.async).getOrElse(SupportedFeatures.asyncClient)
    DalAsyncBatchingConfig(async)
  }

  // read from Zk first. This feature will be disabled if the Zk Value is set to +Inf
  val readCmdLimit: Int =
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

  def partitionMap: PartitionMap =
    config
      .get(PartitionMap.PartitionMapProperty)
      .getOrElse(
        throw new OptimusConfigurationException("Couldn't find the PartitionMap in the RuntimeConfiguration.")
      )
      .asInstanceOf[PartitionMap]

  private class DSIFactoryImplHolder(init: () => DSIFactoryImpl) {
    lazy val impl = init()
  }
  private def createGlobalDSIResolver() = new DSIFactoryImpl {
    private[this] def getGlobalMapping: Map[String, URI] = {
      val mappings = config
        .getProperties(GlobalMappingProperty)
        .getOrElse(
          throw new MissingConfigPropertyException(GlobalMappingProperty)
        )
      val entries = for {
        m <- mappings
        uri = config.getString(List(GlobalMappingProperty, m, "uri").mkString("."))
        if uri.isDefined
      } yield {
        (m, new URI(uri.get))
      }
      entries.toMap
    }
    private val GlobalMappingProperty = "optimus.dsimappings.global"
    lazy private val globalMapping: Map[String, URI] = getGlobalMapping

    override def uriScheme = DSIURIScheme.DSI

    override def createDSI(uri: URI, context: Context) = {
      val newUriOpt = globalMapping.get(uri.getAuthority)

      newUriOpt map { outer.createDSI(_, context) } getOrElse {
        throw new RuntimeException("Unknown DSI authority + " + uri.getAuthority)
      }
    }
  }

  private[optimus] def getPartitionBrokerLookup: PartitionBrokerLookup = PartitionBrokerLookup(config)

  private def createRemoteDSIFactory() = new DSIFactoryImpl {
    override def uriScheme: String = DSIURIScheme.TCP

    def createNonPartitionedDSI(arg: URI, context: Context): DSI = {
      log.info(
        "Partition map received from runtime configuration contains no partitions. " +
          "Creating non partitioned dsi proxy.")
      val queryMap = DSIURIScheme.getQueryMap(arg)
      val pubSubUri = queryMap.get("pubsub").map(hostPort => new URI("tcp://" + hostPort))
      val writer = queryMap.get("writer").map(hostPort => new URI("tcp://" + hostPort))
      val accUri = queryMap.get("acc").map(hostPort => new URI("tcp://" + hostPort))
      val ctx = ClientBrokerContext(context, "", zone, appId, readCmdLimit, asyncConfig, None)
      if (pubSubUri.isEmpty && writer.isEmpty && accUri.isEmpty)
        new RemoteDSIProxy(ctx, arg.getHost, arg.getPort, false, partitionMap, false)
      else {
        new MultiRemoteTcpDsiProxy(
          ClientBrokerContext(context, "", zone, appId, readCmdLimit, asyncConfig, None),
          writer.getOrElse(arg),
          if (writer.nonEmpty) Some(arg) else None,
          pubSubUri,
          accUri,
          partitionMap,
          false // This is non partiioned dsi, legacy code not useful
        )
      }
    }

    def createPartitionedDSI(arg: URI, context: Context): DSI = {
      log.info(s"Partitions to be handled: ${partitionMap.allPartitions}. Creating PartitionedDsiProxy")
      val partitionToLookupMapping = getPartitionBrokerLookup
      val env = DalEnv(config.env)
      val ctx = ClientBrokerContext(
        context,
        "",
        zone,
        appId,
        readCmdLimit,
        asyncConfig,
        ClientBrokerContext.brokerVirtualHostname(env))

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
        false
      ) {
        override def getOrCreateLeaderRemoteDsiProxy(broker: String, servingPartition: Partition): DSIClient =
          new RemoteDSIProxy(ctx, arg.getHost, arg.getPort, false, this.partitionMap, false)
        override def getReplicaRemoteDsiProxy(broker: String, cmdLimit: Int, shouldLB: Boolean): DSIClient = {
          new RemoteDSIProxy(ctx, arg.getHost, arg.getPort, false, this.partitionMap, false)
        }
      }
    }
    override def createDSI(arg: URI, context: Context): DSI = {
      if (isPartitionAware && !partitionMap.partitions.isEmpty) {
        createPartitionedDSI(arg, context)
      } else {
        createNonPartitionedDSI(arg, context)
      }
    }
  }

  private def createReplicaDSIFactory() = new DSIFactoryImpl {

    override def uriScheme = DSIURIScheme.REPLICA

    // We only support Partition config when the partition can be connected using an environment.
    // Connecting to a broker using tcp or ktcp means that we are directly creating a LeaderRemoteDsiProxy and
    // not a DuplexRemoteDsiProxy. It is not possible to resolve the DALProtoClient at that level and a more
    // cleaner approach is to resolve it by connecting to an environment using -e. This would help resolve the
    // partitioning config much before creating the proxies.
    override def createDSI(arg: URI, context: Context) = {
      // sample URI: replica://devny?writer=dev&amp;pubsub=devps
      val replicaUriParts = ReplicaUriParts(arg)
      val shouldLoadBalance = Try(replicaUriParts.loadbalance.getOrElse("false").toBoolean).getOrElse(false)
      val secureTransport = !config.getBoolean(RuntimeProperties.DsiKnownInsecureOutlet).getOrElse(false) &&
        Try(replicaUriParts.secureCon.getOrElse("false").toBoolean).getOrElse(false)
      val env = DalEnv(config.env)
      def getNewPartitionMap: PartitionMap = {
        val unavailablePartitions =
          DSIClient.getUnavailablePartitionsFromReplica(brokerProviderResolver, replicaUriParts.replica)
        if (unavailablePartitions.nonEmpty)
          log.info(s"unavailable partitions: ${unavailablePartitions.mkString(",")}")
        PartitionMap(partitionMap, unavailablePartitions)
      }

      def createNonPartitionedDsiRemoteProxy = {
        replicaUriParts.writer match {
          case Some(writer) =>
            new MultiRemoteDsiProxy(
              context,
              brokerProviderResolver,
              writer,
              replicaUriParts.replica,
              replicaUriParts.pubSub,
              replicaUriParts.acc,
              replicaUriParts.messagesBrk,
              env,
              zone,
              appId,
              partitionMap,
              readCmdLimit,
              asyncConfig,
              shouldLoadBalance,
              secureTransport
            )
          case None =>
            new LeaderRemoteDSIProxy(
              ClientBrokerContext(
                context,
                replicaUriParts.replica,
                zone,
                appId,
                readCmdLimit,
                asyncConfig,
                ClientBrokerContext.brokerVirtualHostname(env)),
              brokerProviderResolver,
              DefaultPartition,
              partitionMap,
              secureTransport
            )
        }
      }

      if (partitionMap.partitions.isEmpty) {
        log.info(
          "Partition map received from runtime configuration contains no partitions. " +
            "Creating non partitioned dsi proxy.")
        createNonPartitionedDsiRemoteProxy
      } else {
        replicaUriParts.writer match {
          case Some(writer) =>
            val newPartitionMap = getNewPartitionMap
            log.info(s"Partitions to be handled: ${newPartitionMap.allPartitions}. Creating PartitionedDsiProxy")
            val partitionToLookupMapping = getPartitionBrokerLookup
            new PartitionedDsiProxy(
              context,
              brokerProviderResolver,
              replicaUriParts.replica,
              writer,
              replicaUriParts.pubSub,
              replicaUriParts.acc,
              replicaUriParts.messagesBrk,
              replicaUriParts.prc.flatMap(DalServicesSilverKingLookup.fromString(env, _)),
              newPartitionMap,
              partitionToLookupMapping,
              zone,
              appId,
              readCmdLimit,
              asyncConfig,
              env,
              shouldLoadBalance,
              secureTransport
            )

          case None =>
            // We *are not* allowing uris without the query "?writer=<something>". If partitioning is being used,
            // the writer must be defined. It's true for the case of live environments. If working from a custom
            // environment, simply specify the uri under the node optimus/<env> under the path,
            // config/optimus/dsi/uri as replica://<envname>?writer=<envname> and partitioning should kick in.
            throw new IllegalArgumentException(s"Writer must be defined in the env uri. Provided uri = $arg")
        }
      }
    }
  }

  private def createBalancingBrokerDSIFactory() = new DSIFactoryImpl {
    override def uriScheme = DSIURIScheme.BROKER

    override def createDSI(arg: URI, context: Context) = {
      new DuplexRemoteBalancingDSIProxy(
        context,
        brokerProviderResolver,
        arg.getAuthority,
        zone,
        appId,
        partitionMap,
        readCmdLimit,
        asyncConfig,
        false)
    }
  }

  private def createDalServiceDiscoveryDsiFactory(): DSIFactoryImpl = new DSIFactoryImpl {
    override def uriScheme: String = DSIURIScheme.SERVICE_DISCOVERY

    override def createDSI(uri: URI, context: Context): DalServiceDiscoveryDsiProxy = {
      val instance = uri.getAuthority
      val path = uri.getPath
      require(
        (instance ne null) && (path ne null),
        s"URI should have authority and path (e.g. $uriScheme://instance/subInstance) but was $uri")
      require(
        path.length > 1 && path.startsWith("/"),
        s"URI should have non-empty path (e.g. $uriScheme://instance/path) but was $uri")
      val subInstance = path.substring(1)
      new DalServiceDiscoveryDsiProxy(
        DalEnv(config.env),
        context,
        partitionMap,
        instance,
        subInstance,
        zone,
        appId,
        readCmdLimit,
        asyncConfig)
    }
  }

  private lazy val runtimeConfigurationClass = classOf[RuntimeConfiguration]

  private[dal] def getDefaultShardUri(uri: URI): URI = {
    val shardConfiguration = Class
      .forName("optimus.dsi.shard.config.ShardConfiguration$")
    val module = shardConfiguration.getField("MODULE$").get(null)
    val apply = module
      .getClass()
      .getMethod(
        "apply",
        uri.getClass,
        runtimeConfigurationClass,
        classOf[Option[AnyRef]],
        classOf[Option[AnyRef]],
        classOf[Option[AnyRef]],
        classOf[Option[AnyRef]])
    val payloadsourceMode = Class.forName("optimus.dsi.shard.config.Read$").getField("MODULE$").get(null)
    val shardConfig = apply.invoke(module, uri, config, Option(payloadsourceMode), None, None, None)
    shardConfig.getClass.getDeclaredMethod("getDefaultSourceUri").invoke(shardConfig).asInstanceOf[URI]
  }

  private lazy val injector = {
    val configurationModule = new AbstractModule {
      override def configure(): Unit = {
        bind(classOf[RuntimeConfiguration]).toInstance(config)
        MapBinder.newMapBinder(binder(), classOf[String], classOf[DSIFactoryImpl])
        val dalAsyncConfigBinder = OptionalBinder.newOptionalBinder(binder(), classOf[DalAsyncConfig])
        asyncConfigOpt.foreach(impl => dalAsyncConfigBinder.setDefault().toInstance(impl))
      }
    }
    val modules = configurationModule +: moduleProviders.map(_.makeModule(config)).toVector
    Guice.createInjector(modules: _*)
  }

  private[this] lazy val factoryMap = {
    val isMock = config.mode == "mock"

    // TODO (OPTIMUS-21937): find the proper way to get classNameStore for AWS env
    if (isMock || config.mode == "aws") {
      Class
        .forName("optimus.dsi.metadata.ClassNameStore")
        .getMethod("createInMemoryClassNameStore")
        .invoke(null)
    }

    val dynamicFactoryMap =
      injector
        .getInstance(Key.get(new TypeLiteral[JMap[String, Provider[DSIFactoryImpl]]]() {}))
        .asScala
        .iterator
        .map({ case (scheme, provider) => scheme -> new DSIFactoryImplHolder(() => provider.get()) })
        .toMap

    dynamicFactoryMap ++ Map(
      DSIURIScheme.DSI -> new DSIFactoryImplHolder(() => createGlobalDSIResolver()),
      DSIURIScheme.TCP -> new DSIFactoryImplHolder(() => createRemoteDSIFactory()),
      DSIURIScheme.SERVICE_DISCOVERY -> new DSIFactoryImplHolder(() => createDalServiceDiscoveryDsiFactory()),
      DSIURIScheme.REPLICA -> new DSIFactoryImplHolder(() => createReplicaDSIFactory()),
      DSIURIScheme.BROKER -> new DSIFactoryImplHolder(() => createBalancingBrokerDSIFactory())
    )
  }

  protected def moduleProviders: Iterator[DsiFactoryModuleProvider] =
    ServiceLoader.load(classOf[DsiFactoryModuleProvider]).iterator().asScala

  protected def getFactory(uri: URI) = {
    val scheme = uri.getScheme
    try {
      val factoryHolder = scheme match {
        case "shard" =>
          val defaultUri = getDefaultShardUri(uri)
          factoryMap(defaultUri.getScheme)

        case _ => factoryMap(scheme)
      }
      Some(factoryHolder.impl)
    } catch {
      case e: Throwable =>
        log.error("Received exception when getFactory: ", e)
        None
    }
  }

  def createDSI(baseUri: URI, context: Context): DSI = {
    val factory = getFactory(baseUri) getOrElse {
      throw new IllegalArgumentException(
        s"No DSI factory for $baseUri, please check if the factory class is in the classpath.")
    }
    factory.createDSI(baseUri, context)
  }

  private[this] def createContext(uri: URI): Context = {
    Context(getQueryMap(uri))
  }

  def createDSI(uri: URI): DSI = {
    createDSI(uri, createContext(uri))
  }

  def createDSI(str: String): DSI = createDSI(makeURI(str))

  private def makeURI(uri: String) = {
    new URI(uri match {
      case "memory://" => "memory://default"
      case _           => uri
    })
  }
}
