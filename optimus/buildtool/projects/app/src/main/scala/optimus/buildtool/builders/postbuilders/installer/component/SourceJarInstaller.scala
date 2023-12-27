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

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.config.NamingConventions.scopeOutputName
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.InstallSourceJar
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.Set

class SourceJarInstaller(installer: Installer) extends ComponentInstaller {
  import installer._

  override val descriptor = "source jars"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installSourceJars(installable.artifacts, installable.includedScopes)

  @async private def installSourceJars(
      artifacts: Seq[Artifact],
      includedScopes: Set[ScopeId]
  ) = {
    val sourceJarArtifacts =
      artifacts.collectAll[InternalClassFileArtifact].filter { a =>
        a.id.tpe == ArtifactType.Sources && includedScopes.contains(a.id.scopeId)
      }
    sourceJarArtifacts.apar.flatMap { a =>
      val scopeId = a.id.scopeId
      val sourceJar = a.file
      val target = pathBuilder.libDir(scopeId).resolveJar(scopeOutputName(scopeId, suffix = "src.jar"))
      bundleFingerprints(a.id.scopeId).writeIfChanged(target, Hashing.hashFileContent(sourceJar)) {
        ObtTrace.traceTask(scopeId, InstallSourceJar) {
          Files.createDirectories(target.parent.path)
          AssetUtils.atomicallyCopy(sourceJar, target, replaceIfExists = true)
        }
      }
    }
  }
}
