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
package optimus.buildtool.builders.postbuilders.installer

import java.util.jar
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.JarAsset
import optimus.buildtool.utils.GitLog
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.JarUtils
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.OsUtils
import optimus.platform._
import optimus.platform.utils.JarManifests.nme._

@entity class ManifestResolver(
    scopeConfigSource: ScopeConfigurationSource,
    val versionConfig: VersionConfiguration,
    pathBuilder: InstallPathBuilder,
    gitLog: Option[GitLog],
) {

  @node final def manifestFromConfig(scopeId: ScopeId): jar.Manifest =
    Jars.createManifest(scopeConfigSource.jarConfiguration(scopeId, versionConfig))

  @async final def locationIndependentManifest(
      scopeId: ScopeId,
      pathingJar: Option[JarAsset],
      installJarMapping: Map[JarAsset, (ScopeId, JarAsset)],
      includeRelativePaths: Boolean = true
  ): Option[jar.Manifest] = pathingJar map { pj =>
    val manifest = Jars
      .readManifestJar(pj)
      .getOrElse(throw new IllegalArgumentException(s"Jar $pathingJar is missing manifest"))
    val maybeAgentPaths = Jars.extractAgentsInManifest(pj, manifest)
    val installedAgents =
      pathBuilder.locationIndependentClasspath(scopeId, maybeAgentPaths, installJarMapping, includeRelativePaths)
    val classpath = Jars.extractManifestClasspath(pj, manifest)
    val installedClasspath =
      pathBuilder.locationIndependentClasspath(
        scopeId,
        classpath,
        installJarMapping,
        includeRelativePaths
      )
    manifest.getMainAttributes.put(jar.Attributes.Name.CLASS_PATH, Jars.manifestPath(installedClasspath))
    manifest.getMainAttributes.remove(JarUtils.nme.PackagedJniLibs)
    manifest.getMainAttributes.remove(JarUtils.nme.PackagedPreloadReleaseLibs)
    manifest.getMainAttributes.remove(JarUtils.nme.PackagedPreloadDebugLibs)
    manifest.getMainAttributes.put(JarUtils.nme.AgentsPath, installedAgents.mkString(";"))
    manifest
  }

  @async def addBuildMetadataAndFingerprint(manifest: jar.Manifest): String = {
    // Attributes#put takes (Object, Object), assuming they are (Name, String)
    // Attributes#putValue takes (String, String), constructing a fresh Name from the first String
    // so we create our own (Name, String) put...
    def put(name: jar.Attributes.Name, value: String): Unit = manifest.getMainAttributes.put(name, value)

    put(PrId, sys.env.getOrElse("PULL_REQUEST_ID", ""))
    put(CiBuildNr, sys.env.getOrElse("BUILD_NUMBER", ""))
    put(BuildUser, sys.props("user.name"))
    put(BuildOSVersion, OsUtils.osVersion)

    put(BuildBranch, gitLog.map(_.branch()).getOrElse(""))
    put(CommitHash, gitLog.flatMap(_.HEAD.map(_.hash)).getOrElse(""))
    put(BaselineCommit, gitLog.flatMap(_.baselineCached().map(_.hash)).getOrElse(""))
    put(BaselineDistance, gitLog.flatMap(_.baselineDistance().map(_.toString)).getOrElse(""))
    put(UncommittedChanges, gitLog.flatMap(_.uncommittedChanges()).map(_ > 0).map(_.toString).getOrElse(""))

    val hash = Hashing.hashStrings(Jars.fingerprint(manifest))
    put(CodeFingerprint, hash)
    hash
  }

}
