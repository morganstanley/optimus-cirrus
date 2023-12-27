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
import optimus.buildtool.app.SkCmdLine
import optimus.buildtool.app.SkWriteCmdLine
import optimus.buildtool.utils.CompilePathBuilder
import optimus.platform._

@entity class SilverKingCacheProvider(
    cmdLine: SkCmdLine,
    defaultVersion: String,
    pathBuilder: CompilePathBuilder
) {
  type SilverKingCache = SimpleArtifactCache[SilverKingStore]

  private def isEnabled[T](sk: String = cmdLine.silverKing): Boolean = sk != OptimusBuildToolCmdLineT.NoneArg
  private val writeCmdLine = PartialFunction.condOpt(cmdLine) { case wcl: SkWriteCmdLine => wcl }

  private val defaultCacheMode: CacheMode = writeCmdLine.fold[CacheMode](CacheMode.ReadOnly) { cmdLine =>
    if (cmdLine.silverKingMode == NoneArg) {
      if (cmdLine.silverKingForceWrite) CacheMode.ForceWrite
      else if (cmdLine.silverKingWritable) CacheMode.ReadWrite
      else CacheMode.ReadOnly
    } else if (cmdLine.silverKingMode == "readWrite") CacheMode.ReadWrite
    else if (cmdLine.silverKingMode == "readOnly") CacheMode.ReadOnly
    else if (cmdLine.silverKingMode == "writeOnly") CacheMode.WriteOnly
    else if (cmdLine.silverKingMode == "forceWrite") CacheMode.ForceWrite
    else if (cmdLine.silverKingMode == "forceWriteOnly") CacheMode.ForceWriteOnly
    else throw new IllegalArgumentException(s"Unrecognized cache mode: ${cmdLine.silverKingMode}")
  }

  @node @scenarioIndependent private[buildtool] def remoteBuildCache: Option[SilverKingCache] =
    if (isEnabled()) { Some(getCache("build")) }
    else None

  @node @scenarioIndependent def remoteBuildDualWriteCache: Option[SilverKingCache] = writeCmdLine.flatMap { cmdLine =>
    val cacheMode = if (cmdLine.silverKingForceWrite) CacheMode.ForceWriteOnly else CacheMode.WriteOnly
    if (isEnabled(cmdLine.silverKingDualWrite)) {
      Some(getCache("build (dual write)", cacheMode = cacheMode, silverKing = cmdLine.silverKingDualWrite))
    } else None
  }

  // root locators are used for "strato catchup" and the local artifact version of OBT at the point catchup is run
  // may not match the artifact version of OBT that build the latest staging, so the remote cache for locators
  // is version independent (artifactVersion = "rootLocator" instead of version)
  @node @scenarioIndependent def remoteRootLocatorCache: Option[SilverKingCache] =
    if (isEnabled()) {
      Some(getCache("rootLocator", version = "rootLocator", offlinePuts = false))
    } else None

  @node @scenarioIndependent def remoteRootLocatorDualWriteCache: Option[SilverKingCache] = writeCmdLine.flatMap {
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
  ): SilverKingCache = {
    val config = SilverKingConfig(silverKing)
    log.info(s"Using silverking $cacheType cache at $config")
    SimpleArtifactCache(SilverKingStore(pathBuilder, config, version, cacheMode.write), cacheMode)
  }
}

object SilverKingCacheProvider {
  // making sure we keep the Silverking caches internal information around to decide if we can write root locators
  `getCache`.setCustomCache(NodeCaching.reallyBigCache)
}
