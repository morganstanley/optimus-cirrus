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
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.InstallBuildPropertiesFiles
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
  private[buildtool] val buildPropertiesFileName = "build.properties"
  private[buildtool] def context(metaBundles: Seq[MetaBundle]): Seq[String] = Seq(
    s"obt-version=${versionConfig.obtVersion}",
    s"scala-version=${versionConfig.scalaVersion}",
    s"install-version=${versionConfig.installVersion}",
    s"stratosphere-version=${versionConfig.stratosphereVersion}"
  ) :+ s"bundles=${metaBundles.map(_.properPath).sorted.mkString(",")}"

  @async override def install(installable: BatchInstallableArtifacts): Seq[FileAsset] = {
    val metaBundles = installable.metaBundles.to(Seq)
    installable.metaBundles.apar.flatMap(installBuildProperties(_, metaBundles)).toIndexedSeq
  }

  private def getBuildPropertiesFile(bundle: MetaBundle): FileAsset =
    installPathBuilder.etcDir(bundle).resolveFile(buildPropertiesFileName)

  private def installBuildProperties(mb: MetaBundle, allMetaBundles: Seq[MetaBundle]): Option[FileAsset] = {
    val content = context(allMetaBundles)
    val hashes = Hashing.hashStrings(content)
    val buildPropertiesFile = getBuildPropertiesFile(mb: MetaBundle)
    bundleFingerprints(mb).writeIfChanged(buildPropertiesFile, hashes) {
      ObtTrace.traceTask(RootScopeId, InstallBuildPropertiesFiles(mb)) {
        Installer.writeFile(buildPropertiesFile, content)
      }
    }
  }

}
