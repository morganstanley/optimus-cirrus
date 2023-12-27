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

import java.nio.file.Files
import java.nio.file.attribute.FileTime
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.crumbs.Properties
import optimus.utils.MiscUtils.Optionable
import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.AnalysisArtifactType
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.ArtifactType._
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.artifacts.LocatorArtifact
import optimus.buildtool.cache.ArtifactReader
import optimus.buildtool.cache.ArtifactWriter
import optimus.buildtool.cache.SearchableArtifactStore
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Commit
import optimus.buildtool.utils.CompilePathBuilder
import optimus.buildtool.utils.GitLog
import optimus.platform._

trait AnalysisLocator {
  @async def saveLocator(
      id: ScopeId,
      analysisType: AnalysisArtifactType,
      pathBuilder: CompilePathBuilder,
      artifactHash: String
  ): Option[LocatorArtifact]

  @async def findAnalysis(
      id: ScopeId,
      analysisType: AnalysisArtifactType,
      incrementalMode: ZincIncrementalMode,
      requireSignatures: Boolean
  ): Option[AnalysisArtifact]
}

private[zinc] object AnalysisLocatorImpl {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}

// case class since this is a ctor parameter to the ScopedCompilationImpl
final case class AnalysisLocatorImpl(
    gitLog: Option[GitLog],
    localArtifactStore: SearchableArtifactStore,
    remoteArtifactReader: Option[ArtifactReader],
    remoteArtifactWriter: Option[ArtifactWriter],
    suppressPersistentStore: Boolean = false)
    extends AnalysisLocator {
  import AnalysisLocatorImpl.log

  /*
  A locator comprises a commit hash (if git is enabled) for the current workspace, together with a timestamp for the build
  the artifact hash that was produced. For every scope built, we store the locator or update its timestamp.
   */
  @async def saveLocator(
      id: ScopeId,
      analysisType: AnalysisArtifactType,
      pathBuilder: CompilePathBuilder,
      artifactHash: String
  ): Option[LocatorArtifact] = {

    if (!suppressPersistentStore) {

      val headCommit = candidateCommits.headOption

      val commitHash = headCommit.map(NamingConventions.COMMIT + _.hash)

      // Save locator locally based on commit hash (if available) and artifact hash. Note that we don't call
      // localArtifactStore.put here, since in production code localArtifactStore will be a FileSystemStore,
      // and FileSystemStore.put is a no-op.
      val localHash = commitHash match {
        case Some(commit) => s"$commit.$artifactHash"
        case None         => artifactHash
      }
      val discriminator = Some(analysisType.name)
      val locatorFile = pathBuilder
        .outputPathFor(id, localHash, ArtifactType.Locator, discriminator, incremental = false)
        .asJson
      val now = patch.MilliInstant.now
      val locator = LocatorArtifact.create(id, analysisType, locatorFile, commitHash, artifactHash, now)
      locator.storeJson()
      // Explicitly set the last modified time on the file so that it exactly matches the timestamp in the locator
      // artifact
      Files.setLastModifiedTime(locatorFile.path, FileTime.from(now))
      log.debug(s"[$id] Locator written: $locator")

      // Save locator remotely (if it doesn't already exist) based on commit hash. In general we will only
      // write to remote stores (eg. SilverKing) from CI jobs, which will always be on a clean commit
      // without local changes (so a one-to-one mapping between commitHash and artifactHash).
      for {
        commit <- commitHash
        store <- remoteArtifactWriter
      } {
        store.put(ArtifactType.Locator)(id, commit, discriminator, locator)
      }

      Some(locator)
    } else None
  }

  /**
   * Find the "best" available analysis file by filtering on recent commits in the workspace git log, and picking the
   * most recent available locator for the most recent git commit that has locators available.
   */
  @async def findAnalysis(
      id: ScopeId,
      analysisType: AnalysisArtifactType,
      incrementalMode: ZincIncrementalMode,
      requireSignatures: Boolean
  ): Option[AnalysisArtifact] = incrementalMode match {
    case ZincIncrementalMode.None =>
      log.debug(s"[$id] No analysis will be used (in non-incremental mode)")
      None
    case ZincIncrementalMode.DryRun =>
      val analysis = _findAnalysis(id, analysisType, Nil, Seq(CompilationFingerprint))
      analysis match {
        case Some(a) =>
          log.debug(s"[$id] Not using analysis (in dry-run mode): $a")
        case None =>
          log.debug(s"[$id] No analysis found (in dry-run mode)")
      }
      None
    case ZincIncrementalMode.Full =>
      val requiredCoreTypes = analysisType match {
        case ScalaAnalysis => Seq(Scala, ScalaMessages)
        case JavaAnalysis  => Seq(Java, JavaMessages)
      }
      val requiredSignatureTypes = if (requireSignatures) Seq(SignatureAnalysis, JavaAndScalaSignatures) else Nil
      // we like to get compilation fingerprints (for debugging purposes), but they're not strictly required to build
      val optionalTypes = Seq(CompilationFingerprint)
      val analysis =
        _findAnalysis(id, analysisType, requiredCoreTypes ++ requiredSignatureTypes, optionalTypes)
      analysis match {
        case Some(a) => log.debug(s"[$id] Using analysis: $a")
        case None    => log.debug(s"[$id] No analysis found")
      }
      analysis
  }

  @async private def _findAnalysis(
      id: ScopeId,
      analysisType: AnalysisArtifactType,
      otherRequiredTypes: Seq[CachedArtifactType],
      optionalTypes: Seq[CachedArtifactType]
  ): Option[AnalysisArtifact] = {
    val discriminator = Some(analysisType.name)
    // sort the local locators in reverse timestamp order, so that we preferentially use
    // recent locators over older ones
    val allLocalLocators =
      localArtifactStore.getAll(id, ArtifactType.Locator, discriminator).sortBy(_.timestamp).reverse

    // Note: commitHashes is reverse ordered by commitTime
    candidateCommits.optionally(_.nonEmpty).flatMap { recentCommits: Seq[Commit] =>
      log.debug(s"[$id] Recent commit hashes: ${recentCommits.mkString(", ")}")
      val commitHashes = recentCommits.map(NamingConventions.COMMIT + _.hash)

      val allLocalLocatorsByCommitHash: Map[String, Seq[LocatorArtifact]] =
        allLocalLocators.flatMap(l => l.commitHash.map(_ -> l)).toGroupedMap

      if (log.isDebugEnabled()) {
        // Ordered by commit, then timestamp
        val orderedLocalLocators = commitHashes.flatMap(c => allLocalLocatorsByCommitHash.getOrElse(c, Nil))
        if (orderedLocalLocators.nonEmpty)
          log.debug(s"[$id] Local commit hashes: ${orderedLocalLocators.map(_.summary).mkString(", ")}")
        else
          log.debug(s"[$id] No local commit hashes found")
      }

      val remoteHashes = remoteArtifactReader
        .map(store => store.check(id, commitHashes.toSet, ArtifactType.Locator, discriminator))
        .getOrElse(Set.empty)

      if (log.isDebugEnabled()) {
        // Ordered by commit
        val orderedRemoteHashes = commitHashes.filter(remoteHashes.contains)
        if (orderedRemoteHashes.nonEmpty)
          log.debug(s"[$id] Remote commit hashes: ${orderedRemoteHashes.mkString(", ")}")
        else
          log.debug(s"[$id] No remote commit hashes found")
      }

      commitHashes
        .filter(h => allLocalLocatorsByCommitHash.contains(h) || remoteHashes.contains(h))
        .optionally(_.nonEmpty)
        .filter(_.nonEmpty)
        .flatMap { availableHashes =>
          log.debug(s"[$id] All available commit hashes: ${availableHashes.mkString(", ")}")

          val lookup = LookupByCommit(
            localArtifactStore,
            remoteArtifactReader,
            id,
            analysisType,
            allLocalLocatorsByCommitHash,
            remoteHashes,
            otherRequiredTypes,
            optionalTypes
          )
          lookup.findAnalysis(availableHashes)
        }
    } orElse {
      if (log.isDebugEnabled()) {
        if (allLocalLocators.nonEmpty)
          log.debug(s"[$id] Timestamp-based local locators:\n  ${allLocalLocators.map(_.summary).mkString("\n  ")}")
        else
          log.debug(s"[$id] No timestamp-based local locators found")
      }
      val lookup =
        LookupByTimestamp(
          localArtifactStore,
          remoteArtifactReader,
          id,
          analysisType,
          allLocalLocators,
          otherRequiredTypes,
          optionalTypes
        )
      lookup.findAnalysis
    }
  }

  @node private def candidateCommits: Seq[Commit] = {
    val ret = gitLog.map(_.recentCommits()).getOrElse(Seq())
    ObtTrace.setProperty(Properties.obtCommit, ret.headOption.fold("NOCOMMIT")(_.hash))
    ret
  }

}

private[zinc] object AnalysisLookup {
  private val log: Logger = getLogger(this.getClass)
}

private[zinc] abstract class AnalysisLookup(
    localArtifactStore: SearchableArtifactStore,
    remoteArtifactStore: Option[ArtifactReader],
    id: ScopeId,
    analysisType: AnalysisArtifactType,
    otherRequiredTypes: Seq[CachedArtifactType],
    optionalTypes: Seq[CachedArtifactType]
) {
  import AnalysisLookup.log

  @async protected def analysisForLocator(locator: LocatorArtifact, locatorType: String): Option[AnalysisArtifact] = {
    val locatorPrefix = if (locatorType.nonEmpty) s"$locatorType " else ""
    log.debug(
      s"[$id] Trying ${locatorPrefix}locator in local analysis store: ${locator.summary} -> ${locator.artifactHash}"
    )
    val localAnalysis = getAnalysisForHash(localArtifactStore, locator.artifactHash)

    val analysis = localAnalysis orElse {
      remoteArtifactStore.flatMap { store =>
        log.debug(
          s"[$id] Trying ${locatorPrefix}locator in remote analysis store: ${locator.summary} -> ${locator.artifactHash}"
        )
        getAnalysisForHash(store, locator.artifactHash)
      }
    }
    analysis.foreach(a => log.debug(s"[$id] Located analysis: $a"))
    analysis
  }

  @async private def getAnalysisForHash(store: ArtifactReader, fingerprintHash: String): Option[AnalysisArtifact] = {
    // Ensure we've got all relevant artifacts for this analysis. Note that if the store is a remote store,
    // then this will also ensure that other remote artifacts are copied locally.
    val (analysis, otherRequiredArtifacts, _) = apar(
      store.get(id, fingerprintHash, analysisType, None),
      otherRequiredTypes.apar.map(t => store.get(id, fingerprintHash, t, None)),
      optionalTypes.apar.map(t => store.get(id, fingerprintHash, t, None))
    )
    if (otherRequiredArtifacts.forall(_.isDefined)) analysis
    else None
  }

  @async protected def findAnalysis(locators: Seq[LocatorArtifact], locatorType: String): Option[AnalysisArtifact] =
    locators match {
      case head +: tail => analysisForLocator(head, locatorType) orElse findAnalysis(tail, locatorType)
      case _            => None
    }
}

object LookupByTimestamp {
  def apply(
      localStore: SearchableArtifactStore,
      remoteStore: Option[ArtifactReader],
      id: ScopeId,
      analysisType: AnalysisArtifactType,
      allLocalLocators: Seq[LocatorArtifact],
      otherRequiredTypes: Seq[CachedArtifactType],
      optionalTypes: Seq[CachedArtifactType]
  ): LookupByTimestamp =
    new LookupByTimestamp(
      localStore,
      remoteStore,
      id,
      analysisType,
      allLocalLocators,
      otherRequiredTypes,
      optionalTypes
    )
}

private[zinc] class LookupByTimestamp(
    localArtifactStore: SearchableArtifactStore,
    remoteArtifactStore: Option[ArtifactReader],
    id: ScopeId,
    analysisType: AnalysisArtifactType,
    allLocalLocators: Seq[LocatorArtifact],
    otherRequiredTypes: Seq[CachedArtifactType],
    optionalTypes: Seq[CachedArtifactType]
) extends AnalysisLookup(localArtifactStore, remoteArtifactStore, id, analysisType, otherRequiredTypes, optionalTypes) {

  @async def findAnalysis: Option[AnalysisArtifact] = findAnalysis(allLocalLocators, "local")
}

object LookupByCommit {
  def apply(
      localStore: SearchableArtifactStore,
      remoteStore: Option[ArtifactReader],
      id: ScopeId,
      analysisType: AnalysisArtifactType,
      allLocalLocatorsByCommitHash: Map[String, Seq[LocatorArtifact]],
      remoteCommitHashes: Set[String],
      otherRequiredTypes: Seq[CachedArtifactType],
      optionalTypes: Seq[CachedArtifactType]
  ): LookupByCommit =
    new LookupByCommit(
      localStore,
      remoteStore,
      id,
      analysisType,
      allLocalLocatorsByCommitHash,
      remoteCommitHashes,
      otherRequiredTypes,
      optionalTypes
    )
}
private[zinc] class LookupByCommit(
    localArtifactStore: SearchableArtifactStore,
    remoteArtifactStore: Option[ArtifactReader],
    id: ScopeId,
    analysisType: AnalysisArtifactType,
    allLocalLocatorsByCommitHash: Map[String, Seq[LocatorArtifact]],
    remoteCommitHashes: Set[String],
    otherRequiredTypes: Seq[CachedArtifactType],
    optionalTypes: Seq[CachedArtifactType]
) extends AnalysisLookup(localArtifactStore, remoteArtifactStore, id, analysisType, otherRequiredTypes, optionalTypes) {

  @async def findAnalysis(commits: Seq[String]): Option[AnalysisArtifact] =
    commits match {
      case head +: tail =>
        val analysisForLocalLocators =
          allLocalLocatorsByCommitHash.get(head).flatMap(findAnalysis(_, "local"))

        val analysis = analysisForLocalLocators orElse {
          if (remoteCommitHashes.contains(head)) {
            remoteArtifactStore.flatMap { store =>
              val remoteLocator = store.get(id, head, ArtifactType.Locator, Some(analysisType.name))
              remoteLocator.flatMap(analysisForLocator(_, "remote"))
            }
          } else None
        }
        analysis orElse findAnalysis(tail)
      case _ =>
        None
    }
}
