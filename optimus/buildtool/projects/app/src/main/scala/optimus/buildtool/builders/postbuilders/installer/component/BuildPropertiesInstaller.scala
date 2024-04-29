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

import optimus.buildtool.builders.postbuilders.installer.BatchInstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.BundleFingerprints
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.FileAsset
import optimus.buildtool.format.Bundle
import optimus.buildtool.trace.CategoryTrace
import optimus.buildtool.trace.InstallBuildPropertiesFiles
import optimus.buildtool.trace.InstallBuildPropertiesRootFile
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Hashing
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.compat._
import scala.collection.immutable.Seq

class BuildPropertiesInstaller(
    installer: Installer,
    installPathBuilder: InstallPathBuilder,
    versionConfig: VersionConfiguration)
    extends ComponentBatchInstaller
    with Log {
  import installer._

  override val descriptor = "build properties"
  private val buildPropertiesFileName = "build.properties"

  @async override def install(installable: BatchInstallableArtifacts): Seq[FileAsset] = {
    val metaBundles = installable.metaBundles.to(Seq)
    val metaBundlesWithEonId: Seq[Bundle] = metaBundles.apar.flatMap(scopeConfigSource.bundleConfiguration)
    val content = Seq(
      s"obt-version=${versionConfig.obtVersion}",
      s"scala-version=${versionConfig.scalaVersion}",
      s"install-version=${versionConfig.installVersion}",
      s"stratosphere-version=${versionConfig.stratosphereVersion}"
    ) :+
      s"bundles=${metaBundles.map(_.properPath).sorted.mkString(",")}" :+
      s"bundlesWithEonId=${metaBundlesWithEonId.filter(p => p.id.eonId.isDefined).map(i => i.id.toStringWithEonId).sorted.mkString(",")}"
    val contentHash = Hashing.hashStrings(content)

    installable.metaBundles.apar
      .flatMap(installMetaBundleBuildProperties(_, content, contentHash))
      .toIndexedSeq ++
      installRootBuildProperties(content, contentHash)
  }

  private def installMetaBundleBuildProperties(
      bundle: MetaBundle,
      content: Seq[String],
      contentHash: String): Option[FileAsset] =
    installBuildProperties(
      installPathBuilder.etcDir(bundle).resolveFile(buildPropertiesFileName),
      bundleFingerprints(bundle),
      InstallBuildPropertiesFiles(bundle),
      content,
      contentHash
    )

  private def installRootBuildProperties(content: Seq[String], contentHash: String): Option[FileAsset] =
    installBuildProperties(
      installDir.resolveFile(buildPropertiesFileName),
      bundleFingerprints(RootScopeId),
      InstallBuildPropertiesRootFile,
      content,
      contentHash)

  private def installBuildProperties(
      target: FileAsset,
      fingerprints: BundleFingerprints,
      category: CategoryTrace,
      content: Seq[String],
      contentHash: String
  ): Option[FileAsset] = fingerprints.writeIfChanged(target, contentHash) {
    ObtTrace.traceTask(RootScopeId, category) {
      Installer.writeFile(target, content)
    }
  }

}
