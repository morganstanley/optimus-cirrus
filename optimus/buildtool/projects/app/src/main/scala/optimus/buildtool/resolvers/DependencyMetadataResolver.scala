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
package optimus.buildtool.resolvers

import optimus.buildtool.config.NamingConventions
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.files.SourceFolderFactory
import optimus.buildtool.files.WorkspaceSourceRoot
import optimus.buildtool.format.ResolverDefinition
import optimus.buildtool.format.ResolverDefinitions
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.TypeClasses.StartingWith
import optimus.platform._

import scala.collection.immutable.IndexedSeq
import scala.collection.immutable.Seq
import optimus.scalacompat.collection._

object DependencyMetadataResolver {

  @node private def toResolvers(
      targets: Seq[ResolverDefinition],
      factory: SourceFolderFactory,
      workspaceSrcRoot: WorkspaceSourceRoot): Seq[DependencyMetadataResolver] = targets.apar.collect {
    case defn if defn.metadataPatterns.nonEmpty && defn.artifactPatterns.nonEmpty =>
      val InWorkspace = new StartingWith(workspaceSrcRoot.pathString + '/')
      val metadataPatterns: IndexedSeq[MetadataPattern] =
        defn.metadataPatterns
          .map(PathUtils.platformIndependentString)
          .apar
          .map {
            case absPattern @ InWorkspace(wsRelPattern) =>
              // the folder we want to watch (srcDirToRepoRoot) is the one right below the first variable
              // i.e., ws/src/optimus/platform/projects/ivy_repo_fixes/ivy-repo/[organisation]/PROJ/[module]/ivy-[revision].xml
              // we want ws/src/optimus/platform/projects/ivy_repo_fixes/ivy-repo
              val beginnings = List(wsRelPattern indexOf '[', wsRelPattern indexOf '(').filter(_ >= 0)
              if (beginnings.isEmpty)
                throw new RuntimeException(s"local metadata pattern $absPattern should have at least one variable")
              val srcDirToRepoRoot = RelativePath(wsRelPattern take beginnings.min)
              val folder = factory.lookupSourceFolder(workspaceSrcRoot, workspaceSrcRoot resolveDir srcDirToRepoRoot)
              MetadataPattern.Local(
                urlPattern = formatAsPatternURL(absPattern),
                urlRepoRoot = formatAsPatternURL(
                  workspaceSrcRoot.resolveDir(folder.workspaceSrcRootToSourceFolderPath).pathString + '/'
                ),
                relPattern = wsRelPattern,
                contents = LocalMetadataRepo.load(folder)
              )
            case absPat =>
              MetadataPattern.Remote(formatAsPatternURL(absPat))
          }(IndexedSeq.breakOut)
      val artifactPatterns: IndexedSeq[ArtifactPattern] =
        defn.artifactPatterns
          .map(PathUtils.platformIndependentString)
          .apar
          .map {
            case absPattern @ InWorkspace(wsRelPattern) =>
              ArtifactPattern.Local(formatAsPatternURL(absPattern), wsRelPattern)
            case absPat =>
              ArtifactPattern.Remote(formatAsPatternURL(absPat))
          }(IndexedSeq.breakOut)
      DependencyMetadataResolver(defn.name, metadataPatterns, artifactPatterns)
  }

  @node def loadResolverConfig(
      factory: SourceFolderFactory,
      workspaceSrcRoot: WorkspaceSourceRoot,
      defns: ResolverDefinitions): DependencyMetadataResolvers =
    DependencyMetadataResolvers(
      toResolvers(defns.defaultIvyResolvers, factory, workspaceSrcRoot),
      toResolvers(defns.defaultMavenResolvers, factory, workspaceSrcRoot),
      toResolvers(defns.onDemandResolvers, factory, workspaceSrcRoot)
    )

  // coursier expects ivy or pom patterns as URLs (and we need them in platform independent format for caching)
  def formatAsPatternURL(path: String): String =
    if (NamingConventions.isHttpOrHttps(path)) path
    else PathUtils.uriString(PathUtils.platformIndependentString(path), absolute = true)
}

final case class DependencyMetadataResolvers(
    defaultIvyResolvers: Seq[DependencyMetadataResolver],
    defaultMavenResolvers: Seq[DependencyMetadataResolver],
    onDemandResolvers: Seq[DependencyMetadataResolver]
) {
  def defaultResolvers: Seq[DependencyMetadataResolver] = defaultIvyResolvers ++ defaultMavenResolvers

  def allResolvers: Seq[DependencyMetadataResolver] = defaultResolvers ++ onDemandResolvers
}

final case class DependencyMetadataResolver(
    name: String,
    metadataPatterns: Seq[MetadataPattern],
    artifactPatterns: Seq[ArtifactPattern]
) {
  lazy val fingerprint: Seq[String] = metadataPatterns.flatMap(_.fingerprint) ++ artifactPatterns.map(_.fingerprint)
}

sealed abstract class MetadataPattern {
  val urlPattern: String
  def fingerprint: Seq[String]
}
object MetadataPattern {
  // just a URL pattern, nothing special
  final case class Remote(urlPattern: String) extends MetadataPattern {
    def fingerprint: Seq[String] = s"matadata-pattern:$urlPattern" :: Nil
  }
  // Pattern to an ivy or pom repo living in codetree
  // Kinda assumes that there's only one pattern per repo, but I think that's reasonable.
  final case class Local(
      /** The full literal pattern as read from resolvers.obt */
      urlPattern: String,
      /** The prefix of `urlPattern` leading up to the beginning of `contents`. */
      urlRepoRoot: String,
      /** The part of the pattern after the source root; used for fingerprinting */
      relPattern: String,
      /** The contents of the repo at the moment we snapshotted it. Used to avoid a potential TOCTOU. */
      contents: LocalMetadataRepo
  ) extends MetadataPattern {
    // urlPattern/urlRepoRoot are *not* stable and cannot make it into a fingerprint
    def fingerprint: Seq[String] = s"local-matadata-pattern:$relPattern" +: contents.fingerprint
  }
}

final case class LocalMetadataRepo(metadataMap: Map[RelativePath, HashedContent]) {
  def localMetadataContent(path: RelativePath): Option[String] = metadataMap.get(path).map(_.utf8ContentAsString)
  // the order of the ivy files in the filesystem has no effect on results, so sort to ensure consistent fingerprints
  lazy val fingerprint: Seq[String] = metadataMap.toIndexedSeq.sortBy(_._1.pathString).map { case (p, c) =>
    s"local-metadata:$p@${c.hash}"
  }
}

@entity object LocalMetadataRepo {
  @node def load(dir: SourceFolder): LocalMetadataRepo = {
    val files = dir.resources(false).apar.map { case (id, hc) => id.sourceFolderToFilePath -> hc }
    LocalMetadataRepo(files)
  }

  val empty = LocalMetadataRepo(Map.empty)
}

sealed abstract class ArtifactPattern {
  val urlPattern: String
  def fingerprint: String
}

object ArtifactPattern {
  // just a URL pattern, nothing special
  final case class Remote(urlPattern: String) extends ArtifactPattern {
    val fingerprint: String = s"artifact-pattern:$urlPattern"
  }

  final case class Local(urlPattern: String, relPattern: String) extends ArtifactPattern {
    // urlPattern is *not* stable and cannot make it into a fingerprint
    val fingerprint: String = s"local-artifact:$relPattern"
  }
}
