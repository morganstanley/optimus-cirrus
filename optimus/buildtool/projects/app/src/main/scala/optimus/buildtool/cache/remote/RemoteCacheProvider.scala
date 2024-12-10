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
package optimus.buildtool.cache.remote

import optimus.buildtool.app.OptimusBuildToolCmdLineT
import optimus.buildtool.app.OptimusBuildToolCmdLineT.NoneArg
import optimus.buildtool.app.RemoteStoreCmdLine
import optimus.buildtool.cache.CacheMode
import optimus.buildtool.cache.MultiWriteableArtifactStore
import optimus.buildtool.cache.MultiWriteArtifactStore
import optimus.buildtool.cache.EmptyStore
import optimus.buildtool.cache.NodeCaching
import optimus.buildtool.cache.RemoteReadThroughTriggeringArtifactCache
import optimus.buildtool.cache.SimpleArtifactCache
import optimus.buildtool.cache.dht.DHTStore
import optimus.buildtool.cache.dht.LocalDhtServer
import optimus.buildtool.cache.silverking.SilverKingConfig
import optimus.buildtool.cache.silverking.SilverKingStore
import optimus.buildtool.utils.CompilePathBuilder
// import optimus.dht.client.api.DHTClientBuilder
import optimus.dht.client.api.registry.StaticRegistryObserver
import optimus.platform._
import optimus.stratosphere.config.StratoWorkspace

@entity class RemoteCacheProvider(
    cmdLine: RemoteCacheProvider.CacheCmdLine,
    defaultVersion: String,
    pathBuilder: CompilePathBuilder,
    stratoWorkspace: StratoWorkspace,
    cacheOperationRecorder: CacheOperationRecorder
) {
  import RemoteCacheProvider._
  private def isEnabled[T](sk: String): Boolean = sk != OptimusBuildToolCmdLineT.NoneArg
  private def isEnabled(): Boolean =
    cmdLine.silverKing != OptimusBuildToolCmdLineT.NoneArg || cmdLine.dhtRemoteStore != OptimusBuildToolCmdLineT.NoneArg
  private val writeCmdLine = PartialFunction.condOpt(cmdLine) { case wcl: RemoteStoreCmdLine => wcl }

  private val defaultCacheMode: CacheMode = writeCmdLine.fold[CacheMode](CacheMode.ReadOnly) { cmdLine =>
    if (cmdLine.remoteCacheMode == NoneArg) {
      if (cmdLine.remoteCacheForceWrite) CacheMode.ForceWrite
      else if (cmdLine.remoteCacheWritable) CacheMode.ReadWrite
      else CacheMode.ReadOnly
    } else if (cmdLine.remoteCacheMode == "readWrite") CacheMode.ReadWrite
    else if (cmdLine.remoteCacheMode == "readOnly") CacheMode.ReadOnly
    else if (cmdLine.remoteCacheMode == "writeOnly") CacheMode.WriteOnly
    else if (cmdLine.remoteCacheMode == "forceWrite") CacheMode.ForceWrite
    else if (cmdLine.remoteCacheMode == "forceWriteOnly") CacheMode.ForceWriteOnly
    else throw new IllegalArgumentException(s"Unrecognized cache mode: ${cmdLine.remoteCacheMode}")
  }

  @node @scenarioIndependent private[buildtool] def remoteBuildCache: Option[RemoteArtifactCache] =
    if (isEnabled()) { Some(getCache("build")) }
    else None

  @node @scenarioIndependent private[buildtool] def readOnlyRemoteBuildCache: Option[RemoteArtifactCache] = {
    defaultCacheMode match {
      case CacheMode(true, _, _) if isEnabled() => Some(getCache("build", cacheMode = CacheMode.ReadOnly))
      case _                                    => None
    }
  }

  @node @scenarioIndependent private[buildtool] def writeOnlyRemoteBuildCache: Option[RemoteArtifactCache] = {
    defaultCacheMode match {
      case CacheMode(_, true, true) if isEnabled()  => Some(getCache("build", cacheMode = CacheMode.ForceWriteOnly))
      case CacheMode(_, true, false) if isEnabled() => Some(getCache("build", cacheMode = CacheMode.WriteOnly))
      case _                                        => None
    }
  }

  // root locators are used for "strato catchup" and the local artifact version of OBT at the point catchup is run
  // may not match the artifact version of OBT that build the latest staging, so the remote cache for locators
  // is version independent (artifactVersion = "rootLocator" instead of version)
  @node @scenarioIndependent def remoteRootLocatorCache: Option[RemoteArtifactCache] =
    if (isEnabled()) {
      Some(getCache("rootLocator", version = "rootLocator", offlinePuts = false))
    } else None

  @node @scenarioIndependent private def getCrossRegionPopulatingCache(
      store: MultiWriteableArtifactStore,
      cacheMode: CacheMode,
      version: String): RemoteArtifactCache = {
    val forcedReadThroughStores: Set[DHTStore] = cmdLine.crossRegionReadThroughDHTLocations.apar
      .withFilter(!_.equalsIgnoreCase(NoneArg))
      .map { location =>
        new DHTStore(pathBuilder, DHTStore.zkClusterType(location), version, cacheMode/* , DHTStore.ZkBuilder(location) */)
      }
      .toSet
    if (forcedReadThroughStores.nonEmpty) {
      RemoteReadThroughTriggeringArtifactCache(
        store,
        forcedReadThroughStores,
        cmdLine.crossRegionDHTSizeThreshold.bytes,
        cacheMode)
    } else {
      SimpleArtifactCache(store, cacheMode)
    }
  }

  @node @scenarioIndependent private def getCache(
      cacheType: String,
      cacheMode: CacheMode = defaultCacheMode,
      version: String = defaultVersion,
      silverKing: String = cmdLine.silverKing,
      offlinePuts: Boolean = true,
  ): RemoteArtifactCache = ??? /* {

    val store = (silverKing, cmdLine.dhtRemoteStore) match {
      case (NoneArg, NoneArg) =>
        log.info(s"SilverKing and DHT remote stores not enabled, using EmptyStore")
        EmptyStore
      case (NoneArg, dht) if dht != NoneArg =>
        log.info(s"Using DHT $cacheType cache at $dht")

        val clusterType = DHTStore.zkClusterType(dht)

        val clientBuilder = LocalDhtServer
          .fromString(dht)
          .map { localServer =>
            val registryObserver = StaticRegistryObserver.local(localServer.port, localServer.uniqueId)
            DHTClientBuilder.create.registryObserver(registryObserver).kerberos(false)
          }
          .getOrElse(DHTStore.ZkBuilder(dht)) // custom ZK path

        new DHTStore(pathBuilder, clusterType, version, cacheMode, clientBuilder)
      case (sk, NoneArg) if sk != NoneArg =>
        val config = SilverKingConfig(silverKing)
        log.info(s"Using silverking $cacheType cache at $config")
        SilverKingStore(pathBuilder, config, version, cacheMode, offlinePuts, cacheOperationRecorder)
      case (sk, dht) if sk != NoneArg && dht != NoneArg =>
        val config = SilverKingConfig(silverKing)
        log.info(s"Using silverking $cacheType cache at $config")
        log.info(s"Using DHT $cacheType cache at $dht")
        log.info(s"Comparison mode enabled, using ComparingArtifactStore")
        new MultiWriteArtifactStore(
          new DHTStore(
            pathBuilder,
            DHTStore.zkClusterType(dht),
            version,
            cacheMode,
            DHTStore.ZkBuilder(dht)
          ),
          Seq(
            SilverKingStore(
              CompilePathBuilder(pathBuilder.outputDir.parent.resolveDir("build_obt_compare")),
              config,
              version,
              cacheMode,
              offlinePuts),
          ),
          stratoWorkspace,
          cacheMode
        )
    }
    getCrossRegionPopulatingCache(store, cacheMode, version)
  } */

}

object RemoteCacheProvider {
  type RemoteArtifactCache =
    SimpleArtifactCache[MultiWriteableArtifactStore]
  type CacheCmdLine = RemoteStoreCmdLine
  // making sure we keep the remote caches internal information around to decide if we can write root locators
  `getCache`.setCustomCache(NodeCaching.reallyBigCache)
}
