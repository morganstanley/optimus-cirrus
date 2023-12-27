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
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompiledRunconfArtifact
import optimus.buildtool.artifacts.CompiledRunconfArtifactType
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.InternalClassFileArtifactType
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.artifacts.PathingArtifactType
import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.files.Asset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.JarAsset
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Jars
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.immutable.Seq
import scala.reflect.ClassTag

/**
 * Some useful utilities for installing build artifacts to somewhere.
 *
 * As it happens these are those things which are needed both for NFS deployment and for Docker image creation.
 */
trait BaseInstaller { this: PostBuilder with Log =>
  protected val versionConfig: VersionConfiguration
  protected val pathBuilder: InstallPathBuilder
  protected val scopeConfigSource: ScopeConfigurationSource
  protected val bundleClassJars: Boolean

  // lazy because of init-order issues with above and a design preference to avoid signature initializers
  protected[installer] final lazy val installVersion = versionConfig.installVersion

  protected def logStart(description: String)(implicit ld: LoggingDetails): Unit =
    log.debug(s"${ld.prefix}Installing $description${ld.midfix}")

  private val nanoToSec = 1000000000.0
  protected def logDuration(description: String, assets: Iterable[Asset], timeInNano: Long)(implicit
      ld: LoggingDetails
  ): Unit = {
    log.debug(f"${ld.prefix}Installed ${assets.size} $description${ld.midfix} in ${timeInNano / nanoToSec}%.1fs")
    log.trace(s"Installed $description: ${assets.mkString(", ")}")
  }

  @node protected final def allScopeArtifacts(artifacts: Seq[Artifact]): Seq[ScopeArtifacts] = {
    val internalArtifacts: Map[InternalArtifactId, JarAsset] =
      artifacts.collect {
        // Just collect the jars here, rather than the artifacts, since in some cases multiple
        // artifacts can contain the same jar with different contentsHashes (due to being retrieved
        // multiple times from different caches), or different values of `containsMacrosOrPlugins`
        // (due to being transitively referenced from different modules)
        case c: InternalClassFileArtifact => (c.id, c.file)
        case p: PathingArtifact           => (p.id, p.pathingFile)
        case r: CompiledRunconfArtifact   => (r.id, r.file)
      }.toDistinctMap

    internalArtifacts
      .groupBy(_._1.scopeId)
      .apar
      .map { case (scopeId, artifactsForScope) =>
        def collectJarsPerType[T <: ArtifactType: ClassTag]: Seq[JarAsset] =
          artifactsForScope.collect { case (InternalArtifactId(_, _: T, _), jar) => jar }.toIndexedSeq

        val classJars = collectJarsPerType[InternalClassFileArtifactType].sortBy(_.pathString)
        val pathingJars = collectJarsPerType[PathingArtifactType]
        val runconfJar = collectJarsPerType[CompiledRunconfArtifactType]

        val installJar =
          if (bundleClassJars && scopeConfigSource.includeInClassBundle(scopeId)) {
            val installJarName = if (scopeId.tpe == "main") "bundle.jar" else s"bundle.${scopeId.tpe}.jar"
            BundleInstallJar(
              scopeId.metaBundle,
              scopeId.tpe,
              pathBuilder.libDir(scopeId.metaBundle).resolveJar(installJarName))
          } else
            ScopeInstallJar(pathBuilder.libDir(scopeId).resolveJar(NamingConventions.scopeOutputName(scopeId)))

        ScopeArtifacts(
          scopeId,
          classJars,
          pathingJars.singleOption,
          runconfJar.singleOption,
          installJar
        )
      }
      .toIndexedSeq
  }

  /** we may have multiple class jars (e.g. java and scala compilations), so merge them */
  protected[installer] final def writeClassJar(
      classJars: Seq[JarAsset],
      manifest: jar.Manifest,
      installJar: JarAsset): Long = {
    // Note: we backup the previous classjar when we write to ensure that any processes running off the previous
    // jar will still be able to classload. This is particularly useful in hot code replace mode, since it prevents
    // a race between replacing the old jar with the new jar and loading new classes.
    AssetUtils.atomicallyWrite(installJar, replaceIfExists = true, localTemp = true, backupPrevious = true) { tempJar =>
      // since this is our final output (which will get distributed to AFS or wherever), we definitely want to compress it
      Jars.mergeJarContentAndManifests(classJars, JarAsset(tempJar), Some(manifest), compress = true)
    }
  }

  /** No manifest version for electron */
  protected[installer] final def writeClassJar(classJars: Seq[JarAsset], installJar: JarAsset): Long = {
    // Note: we backup the previous classjar when we write to ensure that any processes running off the previous
    // jar will still be able to classload. This is particularly useful in hot code replace mode, since it prevents
    // a race between replacing the old jar with the new jar and loading new classes.
    AssetUtils.atomicallyWrite(installJar, replaceIfExists = true, localTemp = true, backupPrevious = true) { tempJar =>
      // since this is our final output (which will get distributed to AFS or wherever), we definitely want to compress it
      Jars.mergeJarContentAndManifests(classJars, JarAsset(tempJar), None, compress = true)
    }
  }
}

sealed trait InstallJar {
  def jar: JarAsset
}
final case class ScopeInstallJar(jar: JarAsset) extends InstallJar
final case class BundleInstallJar(mb: MetaBundle, tpe: String, jar: JarAsset) extends InstallJar

final case class ScopeArtifacts(
    scopeId: ScopeId,
    classJars: Seq[JarAsset],
    pathingJar: Option[JarAsset],
    runconfJar: Option[JarAsset],
    installJar: InstallJar
)

private[installer] final case class LoggingDetails(prefix: String, midfix: String)
