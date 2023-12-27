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
package optimus.buildtool.compilers.zinc

import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.artifacts.ArtifactType.RootLocator
import optimus.buildtool.artifacts.RootLocatorArtifact
import optimus.buildtool.cache.ArtifactReader
import optimus.buildtool.cache.ArtifactWriter
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.utils.CompilePathBuilder
import optimus.buildtool.utils.GitLog
import optimus.platform._

object RootLocatorReader {
  private val log = getLogger(this)
}

/**
 * Methods to search for RootLocators corresponding to git history.
 */
class RootLocatorReader(git: GitLog, reader: ArtifactReader) {
  import RootLocatorReader.log

  /**
   * Finds all RootLocators corresponding to "recent" git commits for the specified branch. Returns the found git
   * commits and their distance from the head of branch.
   */
  @async def findRootLocators(branch: String, recurse: Boolean = false): Seq[(String, Int)] = {
    val orderedCandidateCommits = git.recentCommits(branch).map(_.hash)
    val commitHashKeys = orderedCandidateCommits.map(NamingConventions.COMMIT + _).toSet

    val (cacheTime, matchedCommits) = AdvancedUtils.timed(
      reader
        .check(RootScopeId, commitHashKeys, RootLocator, None)
        .apar
        .filter(k => reader.get(RootScopeId, k, RootLocator, None).isDefined)
        .map(_.stripPrefix(NamingConventions.COMMIT)))
    log.info(f"Cache query took ${cacheTime / 1.0e9}%.2fs")

    val commits = orderedCandidateCommits.zipWithIndex.filter { case (c, _) => matchedCommits(c) }
    if (recurse && commits.size > 1) {
      val (lastHash, lastIndex) = commits.last
      val allCommits = commits ++ findRootLocators(lastHash, recurse = true).map { case (c, i) => (c, lastIndex + i) }
      allCommits.distinct // strip out duplicates from the recursion boundary
    } else commits
  }

  @async def findRootLocatorsAsStrings(branch: String, recurse: Boolean = false): Seq[String] = {
    val orderedMatches = findRootLocators(branch, recurse)
    s"Found ${orderedMatches.size} matches" +: orderedMatches.map { case (c, i) =>
      // this message format is consumed by Stratosphere, so don't mess with it
      // (see optimus.stratosphere.desktop.commands.zinc.CatchUpWithLatestGoodCommit#findBestObtCache)
      s"Cache hit: commit=$c distance=$i"
    }
  }
}

object RootLocatorWriter {
  private val log = getLogger(this)
}

/**
 * Methods to write RootLocators corresponding to git history.
 */
class RootLocatorWriter(git: GitLog, pathBuilder: CompilePathBuilder, writer: ArtifactWriter) {
  import RootLocatorWriter.log

  /**
   * Writes a RootLocator to the store for the current commit. Call this to record the fact that all build artifacts are
   * available for the current commit.
   */
  @async def writeRootLocator(artifactVersion: String): Unit = {
    val headCommit = NamingConventions.COMMIT + git.recentCommits().head.hash
    val asset = pathBuilder.outputPathFor(RootScopeId, headCommit, RootLocator, None, incremental = false).asJson
    val locator = RootLocatorArtifact.create(asset, headCommit, artifactVersion)
    log.info(s"Storing $locator")
    locator.storeJson()
    writer.put(RootLocator)(RootScopeId, headCommit, None, locator)
  }
}
