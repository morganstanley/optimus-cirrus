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
import java.util.jar
import optimus.buildtool.builders.postbuilders.installer.BatchInstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.builders.postbuilders.installer.ManifestResolver
import optimus.buildtool.builders.postbuilders.installer.ScopeArtifacts
import optimus.buildtool.builders.postbuilders.installer.BundleInstallJar
import optimus.buildtool.builders.postbuilders.installer.ScopeInstallJar
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.InstallBundleJar
import optimus.buildtool.trace.InstallJar
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Jars
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.compat._
import scala.collection.immutable.Seq

class ClassJarInstaller(
    installer: Installer,
    manifestResolver: ManifestResolver,
    sparseOnly: Boolean
) extends ComponentInstaller {
  import installer._

  override val descriptor = "class jars"

  @async override def install(installable: InstallableArtifacts): Seq[FileAsset] =
    installJars(installable.includedScopeArtifacts)

  @async private def installJars(includedScopeArtifacts: Seq[ScopeArtifacts]) = {
    val artifacts =
      if (generatePoms) includedScopeArtifacts.filter(_.scopeId.tpe == NamingConventions.MavenInstallScope)
      else includedScopeArtifacts
    artifacts.apar.flatMap { scopeArtifacts =>
      val scopeId = scopeArtifacts.scopeId
      scopeArtifacts.installJar match {
        case ScopeInstallJar(jar)
            if scopeArtifacts.classJars.nonEmpty && (!sparseOnly || !scopeConfigSource.local(scopeId)) =>
          val classJars = scopeArtifacts.classJars
          val installJar =
            if (generatePoms)
              pathBuilder.mavenDir(scopeId).resolveJar(s"${scopeId.module}-${installer.installVersion}.jar")
            else jar
          val manifest = manifestResolver.manifestFromConfig(scopeId)
          val fingerprint = Jars.fingerprint(manifest) ++ classJars.map(Hashing.hashFileContent)
          val hash = Hashing.hashStrings(fingerprint)

          bundleFingerprints(scopeId).writeIfChanged(installJar, hash) {
            ObtTrace.withTraceTask(scopeId, InstallJar) { trace =>
              Files.createDirectories(installJar.parent.path)
              val bytesWritten = writeClassJar(classJars, manifest, installJar)
              trace.addToStat(ObtStats.InstalledJarBytes, bytesWritten)
            }
          }
        case _ => None
      }
    }
  }

}

class BundleJarInstaller(
    installer: Installer,
    manifestResolver: ManifestResolver
) extends ComponentBatchInstaller
    with Log {
  import installer._

  override val descriptor = "bundle class jars"

  @async def install(installable: BatchInstallableArtifacts): Seq[FileAsset] =
    installJars(installable.allScopeArtifacts)

  @async private def installJars(allScopeArtifacts: Seq[ScopeArtifacts]) =
    allScopeArtifacts
      .groupBy(_.installJar)
      .apar
      .flatMap {
        case (BundleInstallJar(mb, tpe, installJar), artifacts) =>
          val classJars = artifacts.flatMap(_.classJars)

          if (classJars.nonEmpty) {
            val manifests = artifacts.apar.map(a => manifestResolver.manifestFromConfig(a.scopeId))
            val manifest = Jars.mergeManifests(manifests).getOrElse(new jar.Manifest)
            val fingerprint = Jars.fingerprint(manifest) ++ classJars.map(Hashing.hashFileContent)
            val hash = Hashing.hashStrings(fingerprint)

            bundleFingerprints(mb).writeIfChanged(installJar, hash) {
              ObtTrace.withTraceTask(RootScopeId, InstallBundleJar(mb)) { trace =>
                log.info(s"[$mb..$tpe] Starting bundle class jar generation from ${classJars.size} jars")
                Files.createDirectories(installJar.parent.path)
                val bytesWritten = writeClassJar(classJars, manifest, installJar)
                trace.addToStat(ObtStats.InstalledJarBytes, bytesWritten)
                log.info(s"[$mb..$tpe] Completing bundle class jar generation")
              }
            }
          } else None
        case _ => None
      }
      .to(Seq)

}
