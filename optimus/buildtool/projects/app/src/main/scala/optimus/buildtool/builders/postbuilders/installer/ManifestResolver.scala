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
import optimus.buildtool.utils.JarUtils
import optimus.buildtool.utils.Jars
import optimus.platform._

@entity class ManifestResolver(
    scopeConfigSource: ScopeConfigurationSource,
    val versionConfig: VersionConfiguration,
    pathBuilder: InstallPathBuilder
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
    manifest
  }

}
