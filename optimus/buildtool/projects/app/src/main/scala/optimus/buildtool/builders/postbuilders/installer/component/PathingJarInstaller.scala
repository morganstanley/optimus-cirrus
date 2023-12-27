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
package optimus.buildtool.builders.postbuilders.installer.component

import java.nio.file.Files
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.builders.postbuilders.installer.ManifestResolver
import optimus.buildtool.builders.postbuilders.installer.ScopeArtifacts
import optimus.buildtool.config.NamingConventions.pathingJarName
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.trace.InstallPathingJar
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.JarUtils
import optimus.buildtool.utils.Jars
import optimus.platform._

import scala.collection.immutable.Seq

class PathingJarInstaller(
    installer: Installer,
    manifestResolver: ManifestResolver
) extends ComponentInstaller {
  import installer._

  override val descriptor = "pathing jars"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installPathingJars(
      installable.allScopeArtifacts,
      installable.includedScopeArtifacts,
      installable.includedRunconfJars
    )

  @async private def installPathingJars(
      allScopeArtifacts: Seq[ScopeArtifacts],
      includedScopeArtifacts: Seq[ScopeArtifacts],
      includedRunconfJars: Seq[(ScopeId, JarAsset)]
  ) = {
    val installJarMapping = InstallJarMapping(allScopeArtifacts)
    val pathingBundleScopes = includedScopeArtifacts.apar.collect {
      case as if scopeConfigSource.scopeConfiguration(as.scopeId).pathingBundle => as.scopeId
    }
    val scopesNeedingPathing = includedRunconfJars.map(_._1).toSet ++ pathingBundleScopes

    includedScopeArtifacts.apar.flatMap {
      case scopeArtifacts: ScopeArtifacts if scopesNeedingPathing.contains(scopeArtifacts.scopeId) =>
        val scopeId = scopeArtifacts.scopeId
        val pathingJar = scopeArtifacts.pathingJar
        val targetDir = pathBuilder.libDir(scopeId)

        val installedPathingJar = targetDir.resolveJar(pathingJarName(scopeId))
        val manifest =
          manifestResolver.locationIndependentManifest(scopeId, pathingJar, installJarMapping).map { m =>
            // this is used by things like OTR when scanning for tests
            m.getMainAttributes.put(JarUtils.nme.ClassJar, scopeArtifacts.installJar.jar.name)
            Jars.mergeManifests(manifestResolver.manifestFromConfig(scopeId), m)
          }

        manifest.toList.flatMap { m =>
          val hash = Hashing.hashStrings(Jars.fingerprint(m))
          bundleFingerprints(scopeId).writeIfChanged(installedPathingJar, hash) {
            ObtTrace.traceTask(scopeId, InstallPathingJar) {
              Files.createDirectories(installedPathingJar.parent.path)
              AssetUtils.atomicallyWrite(installedPathingJar, replaceIfExists = true, localTemp = true) { tempJar =>
                Files.createDirectories(tempJar.getParent)
                Jars.writeManifestJar(JarAsset(tempJar), m)
              }
            }
          }
        }
      case _ => None
    }
  }

}
