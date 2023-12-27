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
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.TypeClasses.StartingWith
import optimus.platform._

import scala.collection.immutable.IndexedSeq
import scala.collection.immutable.Seq
import optimus.scalacompat.collection._

object IvyResolver {
  @node def loadIvyConfig(
      factory: SourceFolderFactory,
      workspaceSrcRoot: WorkspaceSourceRoot,
      defns: Seq[ResolverDefinition]): Seq[IvyResolver] = {
    defns.apar.collect {
      case defn if defn.ivyPatterns.nonEmpty && defn.artifactPatterns.nonEmpty =>
        val InWorkspace = new StartingWith(workspaceSrcRoot.pathString + '/')
        val ivyPatterns: IndexedSeq[IvyPattern] =
          defn.ivyPatterns
            .map(PathUtils.platformIndependentString)
            .apar
            .map {
              case absPattern @ InWorkspace(wsRelPattern) =>
                // the folder we want to watch (srcDirToRepoRoot) is the one right below the first variable
                // i.e., ws/src/optimus/platform/projects/ivy_repo_fixes/ivy-repo/[organisation]/PROJ/[module]/ivy-[revision].xml
                // we want ws/src/optimus/platform/projects/ivy_repo_fixes/ivy-repo
                val beginnings = List(wsRelPattern indexOf '[', wsRelPattern indexOf '(').filter(_ >= 0)
                if (beginnings.isEmpty)
                  throw new RuntimeException(s"local ivy pattern $absPattern should have at least one variable")
                val srcDirToRepoRoot = RelativePath(wsRelPattern take beginnings.min)
                val folder = factory.lookupSourceFolder(workspaceSrcRoot, workspaceSrcRoot resolveDir srcDirToRepoRoot)
                IvyPattern.Local(
                  urlPattern = formatAsPatternURL(absPattern),
                  urlRepoRoot = formatAsPatternURL(
                    workspaceSrcRoot.resolveDir(folder.workspaceSrcRootToSourceFolderPath).pathString + '/'
                  ),
                  relPattern = wsRelPattern,
                  contents = LocalIvyRepo.load(folder)
                )
              case absPat =>
                IvyPattern.Remote(formatAsPatternURL(absPat))
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
        IvyResolver(ivyPatterns, artifactPatterns)
    }
  }

  // coursier expects ivy patterns as URLs (and we need them in platform independent format for caching)
  def formatAsPatternURL(path: String): String =
    if (NamingConventions.isHttpOrHttps(path)) path
    else PathUtils.uriString(PathUtils.platformIndependentString(path), absolute = true)
}

final case class IvyResolver(
    ivyPatterns: Seq[IvyPattern],
    artifactPatterns: Seq[ArtifactPattern]
) {
  lazy val fingerprint: Seq[String] = ivyPatterns.flatMap(_.fingerprint) ++ artifactPatterns.map(_.fingerprint)
}

sealed abstract class IvyPattern {
  val urlPattern: String
  def fingerprint: Seq[String]
}
object IvyPattern {
  // just a URL pattern, nothing special
  final case class Remote(urlPattern: String) extends IvyPattern {
    def fingerprint: Seq[String] = s"ivy-pattern:$urlPattern" :: Nil
  }
  // Pattern to an ivy repo living in codetree
  // Kinda assumes that there's only one pattern per repo, but I think that's reasonable.
  final case class Local(
      /** The full literal pattern as read from resolvers.obt */
      urlPattern: String,
      /** The prefix of `urlPattern` leading up to the beginning of `contents`. */
      urlRepoRoot: String,
      /** The part of the pattern after the source root; used for fingerprinting */
      relPattern: String,
      /** The contents of the repo at the moment we snapshotted it. Used to avoid a potential TOCTOU. */
      contents: LocalIvyRepo
  ) extends IvyPattern {
    // urlPattern/urlRepoRoot are *not* stable and cannot make it into a fingerprint
    def fingerprint: Seq[String] = s"local-ivy-pattern:$relPattern" +: contents.fingerprint
  }
}

final case class LocalIvyRepo(ivys: Map[RelativePath, HashedContent]) {
  def ivyContent(path: RelativePath): Option[String] = ivys.get(path).map(_.utf8ContentAsString)
  // the order of the ivy files in the filesystem has no effect on results, so sort to ensure consistent fingerprints
  lazy val fingerprint: Seq[String] = ivys.toIndexedSeq.sortBy(_._1.pathString).map { case (p, c) =>
    s"local-ivy:$p@${c.hash}"
  }
}

@entity object LocalIvyRepo {
  @node def load(dir: SourceFolder): LocalIvyRepo = {
    val files = dir.resources(false).apar.map { case (id, hc) => id.sourceFolderToFilePath -> hc }
    LocalIvyRepo(files)
  }

  val empty = LocalIvyRepo(Map.empty)
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
