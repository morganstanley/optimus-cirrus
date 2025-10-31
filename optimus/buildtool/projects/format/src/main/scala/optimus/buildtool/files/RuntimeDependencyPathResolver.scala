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
package optimus.buildtool.files

import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.RuntimeDependencyCacheConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.utils.PathUtils

import java.nio.file.Files
import scala.collection.compat._

sealed trait MavenJarAsset {
  val asset: JarAsset
}

final case class CachedMavenJarAsset(asset: JarAsset) extends MavenJarAsset
final case class BundledMavenJarAsset(asset: JarAsset) extends MavenJarAsset

sealed trait DependencyInstallPathResolver {
  val installPathBuilder: InstallPathBuilder

  def resolveDependencyPath(classJar: JarAsset): Option[MavenJarAsset]

  /**
   * Return list of artifacts that still need to be included in `artifactory-deps` inside the bundle.
   * It returns only those artifacts, that are not present in `RuntimeDependencyCache`
   */
  def artifactsToBundle(
      classpath: Seq[JarAsset],
      installJarMapping: Map[JarAsset, (ScopeId, JarAsset)]
  ): Seq[(JarAsset, BundledMavenJarAsset)] = {
    // only copy maven files
    classpath
      .filterNot(installJarMapping.contains)
      .flatMap { dependencyJar =>
        resolveDependencyPath(dependencyJar)
          .map(independentMavenJarAsset => dependencyJar -> independentMavenJarAsset)
      }
      .collect { case (depCopiedAsset: JarAsset, bundledJarAsset: BundledMavenJarAsset) =>
        depCopiedAsset -> bundledJarAsset
      }
      .distinct
  }

  /**
   * Get machine independent paths from the original classpath.
   * This includes module dependencies, external dependencies and paths referring to `RuntimeDependencyCache`
   */
  def locationIndependentClasspath(
      scopeId: ScopeId,
      classpath: Seq[JarAsset],
      installJarMapping: Map[JarAsset, (ScopeId, JarAsset)],
      includeRelativePaths: Boolean = true,
  ): Seq[String] = {
    def isLocalOrInSameDir(localPath: RelativePath) =
      installPathBuilder.installVersion == NamingConventions.LocalVersion || localPath.parentOption.contains(
        RelativePath.empty)

    val pathingDir = installPathBuilder.libDir(scopeId)
    classpath.flatMap { classJar =>
      // Remap jars we have installed to their installed paths. Note that we reference them by relative paths so
      // that the whole installation is location independent
      installJarMapping.get(classJar) match { // Those are just our artifacts
        case Some((jarScopeId, installJar)) =>
          // we've got a path to a jar from another scope in the workspace
          lazy val localPath = pathingDir.relativize(installJar)
          def distedPath: JarAsset =
            installPathBuilder.dirForDist(jarScopeId.metaBundle).resolveDir("lib").resolveJar(installJar.name)
          val paths =
            if (includeRelativePaths) {
              if (isLocalOrInSameDir(localPath))
                // no need to add a disted path when doing a local install or if it's in the same directory as the
                // pathing jar
                Seq(localPath)
              else
                // local path should always be before the disted one
                Seq(localPath, distedPath)
            } else
              // specifically requested no relative paths (because this installation will never be used on NFS-style builds)
              Seq(distedPath)

          paths.map(_.uriString)
        case None => // Those are external dependencies
          // should only be called when localMavenPath exist
          def distedMavenPath: JarAsset = installPathBuilder.getInArtifactoryDepsPath(classJar, disted = true).get.asset

          resolveDependencyPath(classJar) match {
            case Some(CachedMavenJarAsset(asset)) =>
              Seq(asset.uriString) // artifact already deduplicated, no longer need to relativize
            case Some(BundledMavenJarAsset(asset)) =>
              val localPath = pathingDir.relativize(asset) // artifact not yet deduplicated at AFS
              val paths = if (includeRelativePaths) { // WHAT DOES IT MEAN!!!!
                if (isLocalOrInSameDir(localPath))
                  Seq(localPath)
                else Seq(localPath, distedMavenPath)
              } else Seq(distedMavenPath)
              paths.map(_.uriString)
            case _ =>
              val wip = PathUtils.workspaceIndependentPath(classJar.path)
              Seq(PathUtils.uriString(wip.pathString, wip.absolute))
          }
      }
    }.distinct
  }
}

final case class AlwaysBundledDependencyInstallPathResolver(installPathBuilder: InstallPathBuilder)
    extends DependencyInstallPathResolver {

  override def resolveDependencyPath(classJar: JarAsset): Option[MavenJarAsset] =
    installPathBuilder.getInArtifactoryDepsPath(classJar)
}

final case class CachedDependencyInstallPathResolver(
    runtimeDependencyCacheConfiguration: RuntimeDependencyCacheConfiguration,
    installPathBuilder: InstallPathBuilder
) extends DependencyInstallPathResolver {

  def resolveDependencyPath(classJar: JarAsset): Option[MavenJarAsset] = {
    val c = runtimeDependencyCacheConfiguration
    (c.cacheRootPath, c.cachePrefix) match {
      case (Some(cacheRootPath), Some(cachePrefix)) =>
        val maybeCachedPath = InstallPathBuilder
          .getCachedMavenPath(
            classJar,
            cacheRootPath.path,
            cachePrefix,
            c.baseArtifactPath,
            runtimeDependencyCacheConfiguration.releaseRevisions)
          .filter(Files.exists(_))
          .map(cachedArtifactPath => {
            CachedMavenJarAsset(JarAsset(cachedArtifactPath))
          })

        maybeCachedPath.orElse(installPathBuilder.getInArtifactoryDepsPath(classJar))
      case _ => installPathBuilder.getInArtifactoryDepsPath(classJar)
    }
  }

}
