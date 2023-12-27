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
import optimus.buildtool.artifacts.ElectronArtifact
import optimus.buildtool.artifacts.GenericFilesArtifact
import optimus.buildtool.artifacts.ProcessorArtifact
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprints
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.processors.ScopeProcessor
import optimus.buildtool.trace.InstallElectronFiles
import optimus.buildtool.trace.InstallGenericFiles
import optimus.buildtool.trace.InstallProcessedFiles
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.Set

class GenericFileInstaller(installer: Installer, fileInstaller: FileInstaller) extends ComponentInstaller {
  import installer._

  override val descriptor = "generic files"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installGenericFiles(installable.artifacts, installable.includedScopes)

  @async private def installGenericFiles(
      artifacts: Seq[Artifact],
      includedScopes: Set[ScopeId]
  ) = {
    val genericFilesArtifacts =
      artifacts.collectAll[GenericFilesArtifact].filter(a => includedScopes.contains(a.scopeId))
    if (genericFilesArtifacts.nonEmpty) {
      genericFilesArtifacts.apar.flatMap(a => fileInstaller.installFiles(a, bundleFingerprints(a.scopeId)))
    } else Nil
  }
}

class ProcessedFileInstaller(installer: Installer, fileInstaller: FileInstaller) extends ComponentInstaller {
  import installer._

  override val descriptor = "processed files"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installProcessedFiles(installable.artifacts, installable.includedScopes)

  @async private def installProcessedFiles(artifacts: Seq[Artifact], includedScopes: Set[ScopeId]) = {
    val processorArtifacts =
      artifacts.collectAll[ProcessorArtifact].filter(a => includedScopes.contains(a.scopeId))

    if (processorArtifacts.nonEmpty) {
      processorArtifacts.apar.flatMap(a => fileInstaller.installFiles(a, bundleFingerprints(a.scopeId)))
    } else Nil
  }
}

class ElectronInstaller(installer: Installer, installPathBuilder: InstallPathBuilder) extends ComponentInstaller {
  import installer._

  override val descriptor = "electron files"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installElectron(installable.artifacts, installable.includedScopes)

  @async private def installElectron(artifacts: Seq[Artifact], includedScopes: Set[ScopeId]) = {
    val electronArtifacts = artifacts.collectAll[ElectronArtifact].filter(a => includedScopes.contains(a.scopeId))
    electronArtifacts.apar.flatMap(artifact => {
      ObtTrace.traceTask(artifact.scopeId, InstallElectronFiles) {
        Jars.withJar(artifact.file, create = false) { root =>
          val sourceDir = root.resolveDir(RelativePath("common"))
          val sourceFiles = Directory.findFiles(sourceDir)
          sourceFiles
            .map { f =>
              val target =
                installPathBuilder
                  .dirForScope(artifact.scopeId)
                  .resolveDir("bin")
                  .resolveFile(sourceDir.relativize(f).pathString)
              val installed = bundleFingerprints(artifact.scopeId)
                .writeIfChanged(target, Hashing.hashFileContent(f)) {
                  Files.createDirectories(target.parent.path)
                  AssetUtils.atomicallyCopy(f, target, replaceIfExists = true)
                  // Only interested in making executable the executables
                  if (target.name.toLowerCase.startsWith("optimuselectron")) {
                    target.path.toFile.setExecutable(true, false)
                  }
                }
                .isDefined

              (target, installed)
            }
            .collect { case (f, true) => f }
        }
      }
    })
  }
}

class ElectronJarOnlyInstaller(installer: Installer, installPathBuilder: InstallPathBuilder)
    extends ComponentInstaller {
  import installer._

  override val descriptor = "electron jar only"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installElectron(installable.artifacts, installable.includedScopes)

  @async private def installElectron(artifacts: Seq[Artifact], includedScopes: Set[ScopeId]) = {
    val electronArtifacts = artifacts.collectAll[ElectronArtifact].filter(a => includedScopes.contains(a.scopeId))
    electronArtifacts.apar.flatMap(artifact => {
      ObtTrace
        .withTraceTask(artifact.scopeId, InstallElectronFiles) { trace =>
          val target =
            installPathBuilder
              .libDir(artifact.scopeId)
              .resolveJar(NamingConventions.scopeOutputName(artifact.scopeId))
          bundleFingerprints(artifact.scopeId)
            .writeIfChanged(target, Hashing.hashFileContent(artifact.file)) {
              Files.createDirectories(target.parent.path)
              val bytesWritten = writeClassJar(Seq(artifact.file), target)
              trace.addToStat(ObtStats.InstalledJarBytes, bytesWritten)
            }
        }
    })
  }
}

class FileInstaller(installPathBuilder: InstallPathBuilder) {
  @async private[component] def installFiles(
      artifact: GenericFilesArtifact,
      bundleFingerprints: BundleFingerprints
  ): Seq[FileAsset] =
    ObtTrace.traceTask(artifact.scopeId, InstallGenericFiles) {
      val targetDir = installPathBuilder.dirForScope(artifact.scopeId, s"etc/files/${artifact.scopeId.module}")
      val previousTargetFiles = Directory.findFiles(targetDir)
      val newTargetFiles = installFiles(
        targetDir = targetDir,
        sourceRelDir = RelativePath("common"),
        jar = artifact.file,
        bundleFingerprints
      )
      val removedTargetFiles = previousTargetFiles.toSet -- newTargetFiles.map(_._1).toSet

      removedTargetFiles.foreach { f =>
        Files.delete(f.path)
        bundleFingerprints.removeFingerprintHash(f)
      }

      if (removedTargetFiles.nonEmpty) {
        val emptyDirectories = Directory.findDirectories(targetDir).apar.filter { d =>
          Directory.listFiles(d).isEmpty && Directory.listDirectories(d).isEmpty
        }
        emptyDirectories.foreach(d => Files.delete(d.path))
      }

      newTargetFiles.collect { case (f, true) => f }
    }

  @async def installFiles(artifact: ProcessorArtifact, bundleFingerprints: BundleFingerprints): Seq[FileAsset] =
    ObtTrace.traceTask(artifact.scopeId, InstallProcessedFiles) {
      installFiles(
        targetDir = installPathBuilder.dirForScope(artifact.scopeId),
        sourceRelDir = ScopeProcessor.baseRelPath,
        jar = artifact.file,
        bundleFingerprints
      ).collect { case (f, true) => f }
    }

  @async private def installFiles(
      targetDir: Directory,
      sourceRelDir: RelativePath,
      jar: JarAsset,
      bundleFingerprints: BundleFingerprints): Seq[(FileAsset, Boolean)] = {
    Jars.withJar(jar, create = false) { root =>
      val sourceDir = root.resolveDir(sourceRelDir)
      val sourceFiles = Directory.findFiles(sourceDir)
      sourceFiles.map { f =>
        val target = targetDir.resolveFile(sourceDir.relativize(f).pathString)
        val installed = bundleFingerprints
          .writeIfChanged(target, Hashing.hashFileContent(f)) {
            Files.createDirectories(target.parent.path)
            AssetUtils.atomicallyCopy(f, target, replaceIfExists = true)
            // Only interested in making scripts whose direct parent folder is bin executable
            if (target.parent.name == "bin")
              target.path.toFile.setExecutable(true, false)
          }
          .isDefined

        (target, installed)
      }
    }
  }
}
