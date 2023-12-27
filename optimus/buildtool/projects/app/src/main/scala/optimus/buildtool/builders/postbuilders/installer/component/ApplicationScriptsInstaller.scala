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

import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.EndsWithFilter
import optimus.buildtool.files.Directory.Not
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.Directory.PredicateFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.trace.InstallApplicationScripts
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.Utils
import optimus.platform.util.Log
import optimus.platform._

import scala.collection.immutable.Seq

class ApplicationScriptsInstaller(
    installer: Installer,
    pathBuilder: InstallPathBuilder,
    scopeConfigSource: ScopeConfigurationSource,
    minimal: Boolean
) extends ApplicationScriptsInstallerBase
    with ComponentInstaller
    with Log {
  import ApplicationScriptsInstaller._

  override val descriptor = "application scripts"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installApplicationScripts(installable.includedRunconfJars, installable.transitive)

  @async private def installApplicationScripts(
      relevantRunconfJars: Seq[(ScopeId, JarAsset)],
      transitive: Boolean
  ): Seq[FileAsset] = {

    relevantRunconfJars.apar.flatMap {
      case (scopeId, runConfJar) if shouldBeInstalled(scopeId, transitive) =>
        val bundleFingerprints = installer.bundleFingerprints(scopeId)
        // This case is mildly special: we don't actually install runConfJar (which lives in build_obt);
        // we install its contents. We could fingerprint by individual appscripts, but runconf changes are
        // assumed to be rare enough that it's fine to install all appscripts from one scope at the same time.
        // However, since we don't have an actual runconf jar in the install dir, the fingerprint key has to
        // be something other than a filename. This is that key.
        val fpName = s"ApplicationScripts($scopeId)"

        val newHash = Hashing.hashFileContent(runConfJar)
        bundleFingerprints.writeIfKeyChanged[FileAsset](fpName, newHash) {
          ObtTrace.traceTask(scopeId, InstallApplicationScripts) {
            installApplicationScriptsTo(pathBuilder.binDir(scopeId), runConfJar)
          }
        }
      case (scopeId, _) =>
        log.debug(s"[$scopeId] Skipping $descriptor...")
        Nil
    }
  }

  @async private def shouldBeInstalled(scopeId: ScopeId, transitive: Boolean): Boolean =
    !minimal || !transitive || scopeConfigSource.scopeConfiguration(scopeId).flags.installAppScripts

  @async def installDockerApplicationScripts(targetDir: Directory, runconfJar: JarAsset): Seq[FileAsset] = {
    installApplicationScriptsTo(targetDir, runconfJar, fileFilter = dockerScript)
  }

}

class DockerApplicationScriptsInstaller extends ApplicationScriptsInstallerBase {
  @async def installDockerApplicationScripts(targetDir: Directory, runconfJar: JarAsset): Seq[FileAsset] = {
    installApplicationScriptsTo(targetDir, runconfJar, fileFilter = ApplicationScriptsInstaller.dockerScript)
  }
}

trait ApplicationScriptsInstallerBase {
  @async protected def installApplicationScriptsTo(
      targetDir: Directory,
      runconfJar: JarAsset,
      fileFilter: PathFilter = ApplicationScriptsInstaller.nonDockerScript
  ): Seq[FileAsset] = {
    Jars.withJar(runconfJar, create = false) { root =>
      val directoryFilter = EndsWithFilter(RelativePath(root.path.getFileSystem.getPath("bin")))
      val binFolder = Directory.findDirectories(root, directoryFilter)
      binFolder.flatMap { Utils.recursivelyCopy(_, targetDir, Some(Utils.ExecuteBits), fileFilter) }
    }
  }
}

object ApplicationScriptsInstaller {
  val dockerScript = PredicateFilter(_.toString.endsWith(".dckr.sh"))
  val nonDockerScript = Not(dockerScript)
}
