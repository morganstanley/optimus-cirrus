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
import optimus.buildtool.cache.EmptyStore
import optimus.buildtool.cache.NodeCaching
import optimus.buildtool.cache.WriteableArtifactStore
import optimus.buildtool.cache.RemoteReadThroughTriggeringArtifactCache
import optimus.buildtool.cache.SimpleArtifactCache
import optimus.buildtool.cache.dht.DHTStore
import optimus.buildtool.cache.dht.LocalDhtServer
import optimus.buildtool.utils.CompilePathBuilder
import optimus.dht.client.api.DHTClientBuilder
import optimus.dht.client.api.registry.StaticRegistryObserver
import optimus.platform._

@entity class RemoteCacheProvider(
    cmdLine: RemoteCacheProvider.CacheCmdLine,
    defaultVersion: String,
    pathBuilder: CompilePathBuilder
) {
  import RemoteCacheProvider._
  private def isEnabled(): Boolean =
    cmdLine.dhtRemoteStore != OptimusBuildToolCmdLineT.NoneArg
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
      Some(getCache("rootLocator", version = "rootLocator"))
    } else None

  @node @scenarioIndependent private def getCrossRegionPopulatingCache(
      store: WriteableArtifactStore,
      cacheMode: CacheMode,
      version: String): RemoteArtifactCache = {
    val forcedReadThroughStores: Set[DHTStore] = cmdLine.crossRegionReadThroughDHTLocations.apar
      .withFilter(!_.equalsIgnoreCase(NoneArg))
      .map { location =>
        new DHTStore(pathBuilder, DHTStore.zkClusterType(location), version, cacheMode, DHTStore.ZkBuilder(location))
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
      version: String = defaultVersion
  ): RemoteArtifactCache = {

    val store = cmdLine.dhtRemoteStore match {
      case NoneArg =>
        log.info(s"DHT remote store not enabled, using EmptyStore")
        EmptyStore
      case dht =>
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

    }
    getCrossRegionPopulatingCache(store, cacheMode, version)
  }

}

object RemoteCacheProvider {
  type RemoteArtifactCache =
    SimpleArtifactCache[WriteableArtifactStore]
  type CacheCmdLine = RemoteStoreCmdLine
  // making sure we keep the remote caches internal information around to decide if we can write root locators
  `getCache`.setCustomCache(NodeCaching.reallyBigCache)
}
