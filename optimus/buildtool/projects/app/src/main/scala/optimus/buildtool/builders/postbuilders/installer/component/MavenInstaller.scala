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
import optimus.buildtool.builders.postbuilders.installer.ScopeArtifacts
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.trace.InstallJar
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Jars
import optimus.platform._

import scala.collection.immutable.Seq

class MavenInstaller(installer: Installer) extends ComponentInstaller {
  import installer._
  override val descriptor = "maven jars"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installMavenJars(
      installable.allScopeArtifacts,
      installable.includedScopeArtifacts,
      installable.includedRunconfJars
    )

  @async private def installMavenJars(
      allScopeArtifacts: Seq[ScopeArtifacts],
      includedScopeArtifacts: Seq[ScopeArtifacts],
      includedRunconfJars: Seq[(ScopeId, JarAsset)]
  ): Seq[JarAsset] = {
    val installJarMapping = InstallJarMapping(allScopeArtifacts)
    val scopesWithRunconfJar: Set[ScopeId] = includedRunconfJars.map(_._1).toSet

    def copyFile(scopeId: ScopeId, copyFrom: JarAsset, copyTo: JarAsset) = ObtTrace.traceTask(scopeId, InstallJar) {
      Files.createDirectories(copyTo.parent.path)
      // the maven artifacts are immutable, we shouldn't copy them more than once to prevent file lock on windows
      AssetUtils.atomicallyCopy(copyFrom, copyTo, replaceIfExists = false)
    }

    includedScopeArtifacts.apar.flatMap {
      case scopeArtifacts: ScopeArtifacts if scopesWithRunconfJar.contains(scopeArtifacts.scopeId) =>
        val scopeId = scopeArtifacts.scopeId
        val pathingJar = scopeArtifacts.pathingJar
        val mavenAssetOption: Option[Seq[(JarAsset, JarAsset)]] = pathingJar map { pj =>
          val manifest = Jars
            .readManifestJar(pj)
            .getOrElse(throw new IllegalArgumentException(s"Jar $pathingJar is missing manifest"))
          val classpath = Jars.extractManifestClasspath(pj, manifest)
          pathBuilder.mavenDependencyPaths(classpath, installJarMapping)
        }

        mavenAssetOption match {
          case None => Seq.empty
          case Some(artiFiles) =>
            artiFiles.flatMap { case (copyFrom, copyTo) =>
              copyFile(scopeId, copyFrom, copyTo)
              Seq(copyTo)
            }
        }
      case _ => None
    }
  }

}
