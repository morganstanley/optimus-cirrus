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
import optimus.buildtool.artifacts.{ExternalArtifactType => EAT}
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.VersionedExternalArtifactId
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.config.ArchiveConfiguration
import optimus.buildtool.config.ArchiveType
import optimus.buildtool.config.Exclude
import optimus.buildtool.config.NamingConventions.scopeOutputName
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.JarFileSystemAsset
import optimus.buildtool.files.JarHttpAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.resolvers.MavenUtils
import optimus.buildtool.trace.InstallArchive
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.TypeClasses._
import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.immutable.Seq
import scala.collection.immutable.Set

class IntellijPluginInstaller(installer: Installer, archiveInstaller: ArchiveInstaller) extends ComponentInstaller {
  import installer._

  override val descriptor = "intellij plugins"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installable.includedScopes.apar.flatMap(s => installIntellijPlugin(s, installable.artifacts)).toIndexedSeq

  @async private def installIntellijPlugin(scopeId: ScopeId, artifacts: Seq[Artifact]): Seq[FileAsset] = {
    scopeConfigSource.archiveConfiguration(scopeId) match {
      case Some(ArchiveConfiguration(ArchiveType.IntellijPlugin, tokens, excludes)) =>
        val archiveDir = pathBuilder.dirForScope(scopeId, s"${scopeId.module}")
        val target = archiveDir.resolveJar(scopeOutputName(scopeId, "zip"))
        val libDir = s"${scopeId.module}/lib"

        archiveInstaller.installPackageArchive(
          scopeId,
          artifacts,
          target,
          libDir,
          Set(ArchiveType.IntellijPlugin),
          excludes,
          tokens
        ) { versionId =>
          val contentFile = archiveDir.resolveFile("content.properties")
          AssetUtils.atomicallyWrite(contentFile, replaceIfExists = true, localTemp = true) { temp =>
            val content =
              s"""|#Plugin repository for plugin '${scopeId.module}'
                  |plugin.location=${target.name}
                  |plugin.version=$versionId
                  |plugin.local=true
                  |plugin.name=${scopeId.module}
                  |""".stripMargin
            Files.writeString(temp, content)
          }
          Seq(contentFile)
        }
      case _ => Nil
    }
  }
}

class WarInstaller(
    installer: Installer,
    archiveInstaller: ArchiveInstaller,
    warScopes: Set[ScopeId]
) extends ComponentInstaller {
  import installer._

  override val descriptor = "war files"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installable.includedScopes.apar.flatMap(s => installWar(s, installable.artifacts)).toIndexedSeq

  @async private def installWar(scopeId: ScopeId, artifacts: Seq[Artifact]): Seq[FileAsset] = {
    if (warScopes.contains(scopeId)) {
      val target = pathBuilder.dirForScope(scopeId, "war").resolveJar(scopeOutputName(scopeId, "war"))
      val cfg = scopeConfigSource.archiveConfiguration(scopeId).filter(_.archiveType == ArchiveType.War)
      val tokens = cfg.map(_.tokens).getOrElse(Map.empty)
      val excludes = cfg.map(_.excludes).getOrElse(Set.empty)
      val installedWarFiles =
        archiveInstaller.installPackageArchive(scopeId, artifacts, target, "WEB-INF/lib", Set.empty, excludes, tokens)()
      installedWarFiles
    } else Nil
  }
}

class ArchiveInstaller(installer: Installer) extends Log {
  import installer._

  @async private[component] def installPackageArchive(
      scopeId: ScopeId,
      artifacts: Seq[Artifact],
      target: JarAsset,
      libDir: String,
      excludedUpstreamArchives: Set[ArchiveType],
      excludes: Set[Exclude],
      tokens: Map[String, String]
  )(extraFiles: String => Seq[FileAsset] = _ => Nil): Seq[FileAsset] = {
    val classFileArtifacts = artifacts.collectAll[InternalClassFileArtifact]

    val scope = factory.scope(scopeId)

    val internalDepScopes = scope.runtimeDependencies.transitiveScopeDependencies.map(_.id).toSet

    // If configured, ensure we don't include dependencies from upstream archives in our own package (in intellij
    // for example, this causes problems due to the same class being loaded in multiple classloaders)
    val upstreamArchives = scope.runtimeDependencies.transitiveScopeDependencies.filter { s =>
      scopeConfigSource.archiveConfiguration(s.id).exists(cfg => excludedUpstreamArchives.contains(cfg.archiveType))
    }.toSet

    val upstreamArchiveInternalDepScopes = upstreamArchives.apar
      .flatMap { s =>
        s +: s.runtimeDependencies.transitiveScopeDependencies
      }
      .map(_.id)
    val upstreamArchiveExternalDeps = upstreamArchives.apar.flatMap { s =>
      s.runtimeDependencies.transitiveExternalDependencies.result.resolvedArtifacts
    }

    val (archiveContent, internalDeps) = classFileArtifacts
      .filter { a =>
        val artifactScopeId = a.id.scopeId
        (artifactScopeId == scopeId || internalDepScopes.contains(artifactScopeId)) &&
        !upstreamArchiveInternalDepScopes.contains(artifactScopeId)
      }
      .partition { a =>
        a.id.scopeId == scopeId && a.id.tpe == ArtifactType.ArchiveContent
      }
    val externalDeps = scope.runtimeDependencies.transitiveExternalDependencies.result.resolvedArtifacts
      .filter { a =>
        !upstreamArchiveExternalDeps.contains(a)
      }
      .filter {
        _.id match {
          case VersionedExternalArtifactId(group, name, _, _, EAT.SignatureJar | EAT.ClassJar) =>
            !excludes.exists { e =>
              e.group.forall(_ == group) && e.name.forall(_ == name)
            }
          case _ => true
        }
      }

    val fingerprint = (archiveContent ++ internalDeps ++ externalDeps).map { a =>
      PathUtils.fingerprintAsset("", a.file)
    } ++
      tokens.map { case (k, v) => s"[Token]$k=$v" } :+
      s"[InstallVersion]$installVersion"

    val hash = Hashing.hashStrings(fingerprint)

    val versionId = installVersion match {
      case "local" => s"$installVersion-$hash"
      case _       => installVersion
    }

    val resolvedTokens = tokens.map { case (k, v) =>
      k -> v
        .replace("obt.installVersion", installVersion)
        .replace("obt.versionId", versionId)
    }

    val installedArchive = bundleFingerprints(scopeId).writeIfChanged(target, hash) {
      ObtTrace.traceTask(scopeId, InstallArchive) {
        Files.createDirectories(target.parent.path)
        AssetUtils.atomicallyWrite(target, replaceIfExists = true, localTemp = true) { tempJar =>
          val jarContents = archiveContent.map(_.file)
          // since this is our final output (which will get distributed to AFS or wherever), we definitely want to compress it
          Jars.mergeContent(jarContents, JarAsset(tempJar), resolvedTokens, compress = true) { output =>
            val max = (internalDeps ++ externalDeps).size - 1
            // prefix with index to prevent dupes and to ensure some level of classpath ordering
            internalDeps.zipWithIndex.foreach { case (a, i) =>
              val idx = Utils.sortableInt(i, max)
              output.copyInFile(a.path, RelativePath(s"$libDir/$idx-${a.id.tpe.name}-${a.file.name}"))
            }
            val externalOffset = internalDeps.size
            externalDeps.zipWithIndex.foreach { case (a, i) =>
              val idx = Utils.sortableInt(i + externalOffset, max)
              val filePath = a.file match {
                case f: JarHttpAsset =>
                  MavenUtils
                    .getLocal(f, installer.depCopyRoot)
                    .getOrThrow(s"Get http file failed! it should be downloaded into local disk but not: ${f.url}")
                    .path
                case _: JarFileSystemAsset => a.path
              }
              output.copyInFile(filePath, RelativePath(s"$libDir/$idx-${a.file.name}"))
            }
          }
        }
      }
    }

    installedArchive.toIndexedSeq.flatMap { a =>
      a +: extraFiles(versionId)
    }
  }
}
