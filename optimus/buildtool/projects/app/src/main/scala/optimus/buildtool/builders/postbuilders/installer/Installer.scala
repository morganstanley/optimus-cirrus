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

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import optimus.buildtool.app.ScopedCompilationFactory
import optimus.buildtool.artifacts.Artifact.InternalArtifact
import optimus.buildtool.artifacts._
import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.builders.postbuilders.installer.component._
import optimus.buildtool.builders.postinstallers.PostInstaller
import optimus.buildtool.config.GenericRunnerConfiguration
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ObtConfig
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.TestplanConfiguration
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.files.Directory
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.WorkspaceSourceRoot
import optimus.buildtool.format.WorkspaceStructure
import optimus.buildtool.trace._
import optimus.buildtool.utils.GitLog
import optimus.platform._
import optimus.platform.util.Log

import scala.collection.immutable.Seq
import scala.collection.immutable.Set

/**
 * A post-build step which copies all installable artifacts to install paths and generates location-independent pathing
 * jars.
 *
 * Our install paths are:
 *
 * "<meta>/<proj>/<version>/common/lib/<module>[.<type if not "main">].jar" for class jars, and
 * "<meta>/<proj>/<version>/common/lib/<module>[.<type if not "main">]-runtimeAppPathing.jar" for pathing jars
 *
 * (where version defaults to "local" unless it's overridden on the OBT command line; e.g. in release builds)
 */
class Installer(
    protected[installer] val scopeConfigSource: ScopeConfigurationSource,
    private[installer] val factory: ScopedCompilationFactory,
    private[installer] val installDir: Directory,
    private[installer] val depCopyRoot: Directory,
    private[installer] val sourceDir: WorkspaceSourceRoot,
    protected val versionConfig: VersionConfiguration,
    private[installer] val testplanConfig: TestplanConfiguration,
    genericRunnerConfiguration: GenericRunnerConfiguration,
    postInstaller: PostInstaller,
    private[installer] val gitLog: Option[GitLog],
    private[installer] val installTestplans: Boolean,
    private[installer] val directoryFactory: DirectoryFactory,
    sparseOnly: Boolean = false,
    minimal: Boolean = false,
    warScopes: Set[ScopeId] = Set.empty,
    bundleFingerprintsCache: BundleFingerprintsCache,
    protected[installer] val generatePoms: Boolean = false,
    protected val bundleClassJars: Boolean = false
) extends BaseInstaller
    with PostBuilder
    with Log {

  protected[installer] val pathBuilder: InstallPathBuilder =
    if (generatePoms) InstallPathBuilder.mavenRelease(installDir, installVersion)
    else InstallPathBuilder.dev(installDir, installVersion)

  private val installedFiles = new AtomicInteger(0)

  private val testplanInstaller: Option[TestplanInstaller] =
    if (installTestplans) {
      val workspaceStructure = scopeConfigSource match {
        case obtConfig: ObtConfig => obtConfig.workspaceStructure
        case _                    => WorkspaceStructure.Empty
      }
      Some(
        new TestplanInstaller(
          this,
          versionConfig,
          sys.env.get("STRATOSPHERE_INFRA_OVERRIDE"),
          workspaceStructure
        ))
    } else None

  private val components = {
    val manifestResolver: ManifestResolver = ManifestResolver(scopeConfigSource, versionConfig, pathBuilder)
    val classJarInstaller = new ClassJarInstaller(this, manifestResolver, sparseOnly)
    if (sparseOnly)
      Seq(
        classJarInstaller,
        new SourceJarInstaller(this)
      )
    else if (generatePoms)
      Seq(
        classJarInstaller,
        new MavenPomInstaller(
          scopeConfigSource,
          bundleFingerprintsCache,
          pathBuilder,
          installVersion,
          NamingConventions.MavenInstallScope
        )
      )
    else {
      val fileInstaller = new FileInstaller(pathBuilder)
      val archiveInstaller = new ArchiveInstaller(this)
      val ivyInstaller = new IvyInstaller(scopeConfigSource, bundleFingerprintsCache, pathBuilder, installVersion)
      Seq(
        classJarInstaller,
        new SourceJarInstaller(this),
        ivyInstaller,
        new PathingJarInstaller(this, manifestResolver),
        new CppInstaller(pathBuilder, bundleFingerprintsCache),
        new ApplicationScriptsInstaller(this, pathBuilder, scopeConfigSource, minimal),
        new CopyFilesInstaller(bundleFingerprintsCache, scopeConfigSource, sourceDir, pathBuilder, directoryFactory),
        new GenericFileInstaller(this, fileInstaller),
        new ProcessedFileInstaller(this, fileInstaller),
        new IntellijPluginInstaller(this, archiveInstaller),
        new WarInstaller(this, archiveInstaller, warScopes),
        new MavenInstaller(this),
        new ElectronJarOnlyInstaller(this, pathBuilder)
      ) ++ testplanInstaller
    }
  }

  private val batchComponents =
    if (generatePoms) Seq.empty
    else {
      val bundleJarInstaller = if (bundleClassJars) {
        val manifestResolver = ManifestResolver(scopeConfigSource, versionConfig, pathBuilder)
        Some(new BundleJarInstaller(this, manifestResolver))
      } else None
      Seq(
        new VersionInstaller(bundleFingerprintsCache, pathBuilder, installVersion, sys.env.get("GIT_COMMIT")),
        new BundleRunConfsInstaller(this, factory, pathBuilder),
        new MultiBundleJarInstaller(this),
        new GenericRunnerInstaller(bundleFingerprintsCache, pathBuilder, genericRunnerConfiguration),
        new DependencyFileInstaller(this),
        new BuildPropertiesInstaller(this, pathBuilder, versionConfig)
      ) ++ bundleJarInstaller ++ testplanInstaller
    }

  private[installer] def bundleFingerprints(scopeId: ScopeId): BundleFingerprints =
    bundleFingerprints(scopeId.metaBundle)
  private[installer] def bundleFingerprints(mb: MetaBundle): BundleFingerprints =
    bundleFingerprintsCache.bundleFingerprints(mb)

  @async override def postProcessScopeArtifacts(id: ScopeId, artifacts: Seq[Artifact]): Unit = {
    val assets = installArtifacts(Set(id), artifacts, transitive = false)
    postInstaller.postInstallFiles(Set(id), assets, successful = !Artifact.hasErrors(artifacts))
  }

  @async override def postProcessTransitiveArtifacts(transitiveScopes: Set[ScopeId], artifacts: Seq[Artifact]): Unit = {
    val assets = installArtifacts(transitiveScopes, artifacts, transitive = true)
    postInstaller.postInstallFiles(transitiveScopes, assets, successful = !Artifact.hasErrors(artifacts))
  }

  @async private def installArtifacts(
      includedScopes: Set[ScopeId],
      artifacts: Seq[Artifact],
      transitive: Boolean
  ): Seq[FileAsset] = {
    Files.createDirectories(installDir.path)

    val installable =
      InstallableArtifacts(includedScopes, artifacts, allScopeArtifacts(artifacts), transitive)

    implicit val ld: LoggingDetails = includedScopes.singleOrNone match {
      case Some(id) if !transitive => LoggingDetails(s"[$id] ", "")
      case _                       => LoggingDetails("", s" for ${includedScopes.size} transitive scopes")
    }

    val assets = components.apar.flatMap { c =>
      logStart(c.descriptor)
      val (time, files) = AdvancedUtils.timed {
        c.install(installable)
      }
      logDuration(c.descriptor, files, time)
      files
    }
    installedFiles.addAndGet(assets.size)
    assets
  }

  @async override def postProcessArtifacts(scopes: Set[ScopeId], artifacts: Seq[Artifact], successful: Boolean): Unit =
    if (successful && !sparseOnly) {
      implicit val ld: LoggingDetails = LoggingDetails("", "")

      val installable = BatchInstallableArtifacts(artifacts, allScopeArtifacts(artifacts))

      val assets = batchComponents.apar.flatMap { c =>
        logStart(c.descriptor)
        val (time, files) = AdvancedUtils.timed {
          c.install(installable)
        }
        logDuration(c.descriptor, files, time)
        files
      }
      installedFiles.addAndGet(assets.size)

      postInstaller.postInstallFiles(assets, successful)
      postInstaller.complete(successful)

      val totalInstalled = installedFiles.getAndSet(0)
      val msg = s"$totalInstalled artifacts installed to $installDir"
      ObtTrace.addToStat(ObtStats.InstalledJars, totalInstalled)
      log.info(msg)
      ObtTrace.info(msg)
    }

  override def save(successful: Boolean): Unit = {
    bundleFingerprintsCache.save()
    components.foreach(_.save())
  }

}

object Installer {
  import optimus.buildtool.utils.AssetUtils
  import scala.jdk.CollectionConverters._

  def writeFile(targetFile: FileAsset, content: Seq[String]): Unit = {
    Files.createDirectories(targetFile.parent.path)
    AssetUtils.atomicallyWrite(targetFile, replaceIfExists = true, localTemp = true) { tempPath =>
      Files.write(tempPath, content.asJava)
    }
  }
}

final case class InstallableArtifacts(
    includedScopes: Set[ScopeId],
    artifacts: Seq[Artifact],
    allScopeArtifacts: Seq[ScopeArtifacts],
    transitive: Boolean
) {
  def includedArtifacts(tpe: CachedArtifactType): Seq[tpe.A] = artifacts.collect {
    case InternalArtifact(id, a) if id.tpe == tpe && includedScopes.contains(id.scopeId) => a.asInstanceOf[tpe.A]
  }
  val includedScopeArtifacts: Seq[ScopeArtifacts] = allScopeArtifacts.filter(a => includedScopes.contains(a.scopeId))

  val compiledRunconfJars: Seq[(ScopeId, JarAsset)] = allScopeArtifacts.flatMap { scopeArtifacts =>
    scopeArtifacts.runconfJar.map(jar => scopeArtifacts.scopeId -> jar)
  }
  val includedRunconfJars: Seq[(ScopeId, JarAsset)] = compiledRunconfJars.filter { case (scopeId, _) =>
    includedScopes.contains(scopeId)
  }
}

final case class BatchInstallableArtifacts(artifacts: Seq[Artifact], allScopeArtifacts: Seq[ScopeArtifacts]) {
  val allScopes: Set[ScopeId] = Artifact.scopeIds(artifacts).toSet
  val metaBundles: Set[MetaBundle] = allScopes.map(_.metaBundle)
}
