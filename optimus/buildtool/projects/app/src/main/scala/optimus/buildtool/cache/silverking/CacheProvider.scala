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
package optimus.buildtool.cache
package silverking

import optimus.buildtool.app.OptimusBuildToolCmdLineT
import optimus.buildtool.app.OptimusBuildToolCmdLineT.NoneArg
import optimus.buildtool.app.RemoteStoreCmdLine
import optimus.buildtool.app.RemoteStoreWriteCmdLine
import optimus.buildtool.cache.CacheMode
import optimus.buildtool.cache.dht.DHTStore
import optimus.buildtool.utils.CompilePathBuilder
import optimus.platform._
import optimus.stratosphere.config.StratoWorkspace

@entity class CacheProvider(
    cmdLine: CacheProvider.CacheCmdLine,
    defaultVersion: String,
    pathBuilder: CompilePathBuilder,
    stratoWorkspace: StratoWorkspace,
    cacheOperationRecorder: CacheOperationRecorder
) {
  import CacheProvider._
  private def isEnabled[T](sk: String): Boolean = sk != OptimusBuildToolCmdLineT.NoneArg
  private def isEnabled(): Boolean =
    cmdLine.silverKing != OptimusBuildToolCmdLineT.NoneArg || cmdLine.dhtRemoteStore != OptimusBuildToolCmdLineT.NoneArg
  private val writeCmdLine = PartialFunction.condOpt(cmdLine) { case wcl: RemoteStoreWriteCmdLine => wcl }

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

  @node @scenarioIndependent def remoteBuildDualWriteCache: Option[RemoteArtifactCache] = writeCmdLine.flatMap {
    cmdLine =>
      val cacheMode = if (cmdLine.remoteCacheForceWrite) CacheMode.ForceWriteOnly else CacheMode.WriteOnly
      if (isEnabled(cmdLine.silverKingDualWrite)) {
        Some(getCache("build (dual write)", cacheMode = cacheMode, silverKing = cmdLine.silverKingDualWrite))
      } else None
  }

  // root locators are used for "strato catchup" and the local artifact version of OBT at the point catchup is run
  // may not match the artifact version of OBT that build the latest staging, so the remote cache for locators
  // is version independent (artifactVersion = "rootLocator" instead of version)
  @node @scenarioIndependent def remoteRootLocatorCache: Option[RemoteArtifactCache] =
    if (isEnabled()) {
      Some(getCache("rootLocator", version = "rootLocator", offlinePuts = false))
    } else None

  @node @scenarioIndependent def remoteRootLocatorDualWriteCache: Option[RemoteArtifactCache] = writeCmdLine.flatMap {
    cmdLine =>
      if (isEnabled(cmdLine.silverKingDualWrite)) {
        Some(
          getCache(
            "rootLocator (dual write)",
            version = "rootLocator",
            silverKing = cmdLine.silverKingDualWrite,
            offlinePuts = false))
      } else None
  }

  @node @scenarioIndependent private def getCache(
      cacheType: String,
      cacheMode: CacheMode = defaultCacheMode,
      version: String = defaultVersion,
      silverKing: String = cmdLine.silverKing,
      offlinePuts: Boolean = true
  ): RemoteArtifactCache = {

    val store: ComparableArtifactStore = (silverKing, cmdLine.dhtRemoteStore, cmdLine.comparisonMode) match {
      case (NoneArg, NoneArg, _) =>
        log.info(s"SilverKing and DHT remote stores not enabled, using EmptyStore")
        EmptyStore
      case (NoneArg, dht, _) if dht != NoneArg =>
        log.info(s"Using DHT $cacheType cache at $dht")
        new DHTStore(pathBuilder, DHTStore.zkClusterType(dht), version, cacheMode.write, DHTStore.ZkBuilder(dht))
      case (sk, NoneArg, _) if sk != NoneArg =>
        val config = SilverKingConfig(silverKing)
        log.info(s"Using silverking $cacheType cache at $config")
        SilverKingStore(pathBuilder, config, version, cacheMode.write, offlinePuts, cacheOperationRecorder)
      case (sk, dht, true) if sk != NoneArg && dht != NoneArg =>
        val config = SilverKingConfig(silverKing)
        log.info(s"Using silverking $cacheType cache at $config")
        log.info(s"Using DHT $cacheType cache at $dht")
        log.info(s"Comparison mode enabled, using ComparingArtifactStore")
        new ComparingArtifactStore(
          new DHTStore(
            pathBuilder,
            DHTStore.zkClusterType(dht),
            version,
            cacheMode.write,
            DHTStore.ZkBuilder(dht)
          ),
          Seq(
            SilverKingStore(
              CompilePathBuilder(pathBuilder.outputDir.parent.resolveDir("build_obt_compare")),
              config,
              version,
              cacheMode.write,
              offlinePuts),
          ),
          stratoWorkspace
        )
      case (sk, dht, false) if sk != NoneArg && dht != NoneArg =>
        val config = SilverKingConfig(silverKing)
        log.info(s"Using silverking $cacheType cache at $config")
        log.info(s"Using DHT $cacheType cache at $dht")
        log.info(s"SilverKing and DHT remote stores enabled without comparison mode, using DualStore")
        throw new UnsupportedOperationException("Not implemented yet, specify --comparisonMode")
    }
    SimpleArtifactCache(store, cacheMode)
  }

}

object CacheProvider {
  type RemoteArtifactCache =
    SimpleArtifactCache[ComparableArtifactStore]
  type CacheCmdLine = RemoteStoreCmdLine
  // making sure we keep the Silverking caches internal information around to decide if we can write root locators
  `getCache`.setCustomCache(NodeCaching.reallyBigCache)
}
