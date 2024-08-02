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
package optimus.buildtool.builders

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.app.ScopedCompilationFactory
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.Artifact.InternalArtifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.InMemoryMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.artifacts.Severity
import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.builders.postinstallers.uploaders.AssetUploader
import optimus.buildtool.builders.reporter.MessageReporter
import optimus.buildtool.cache.RemoteArtifactCacheTracker
import optimus.buildtool.compilers.CompilationException
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.resolvers.DependencyDownloadTracker
import optimus.buildtool.scope.ScopedCompilation
import optimus.buildtool.trace._
import optimus.buildtool.utils.FileDiff
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.Utils.OsVersionMismatch
import optimus.graph.CancellationScope
import optimus.graph.OptimusCancellationException
import optimus.graph.cache.Caches
import optimus.graph.diagnostics.EvictionReason
import optimus.platform._

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.util.control.NonFatal

sealed trait BuildResult {
  def artifacts: Seq[Artifact]
  def messageArtifacts: Seq[MessagesArtifact]
  def errors: Int
  def successful: Boolean = errors == 0
}
object BuildResult {
  final case class CompletedBuildResult(
      artifacts: Seq[Artifact],
      modifiedFiles: Option[FileDiff]
  ) extends BuildResult {

    override val messageArtifacts: Seq[MessagesArtifact] = Artifact.messages(artifacts)
    val scopeIds: Seq[ScopeId] = Artifact.scopeIds(artifacts)

    val errorsByArtifact: Seq[(MessagesArtifact, Seq[CompilationMessage])] = {
      messageArtifacts.flatMap { a =>
        val errors = a.messages.collect {
          case m if m.severity == Severity.Error =>
            m
          case m if m.isNew && m.pos.exists(p => modifiedFiles.exists(_.contains(p.filepath))) =>
            m.copy(severity = Severity.Error)
        }
        if (errors.nonEmpty) Some(a -> errors) else None
      }
    }

    val errorArtifacts: Seq[MessagesArtifact] = errorsByArtifact.map(_._1)
    val failedScopes: Seq[ScopeId] = errorArtifacts.map(_.id.scopeId).distinct
    override val errors: Int = errorsByArtifact.map(_._2.size).sum
  }
  final case class AbortedBuildResult(t: Throwable) extends BuildResult {
    override def artifacts: Seq[Artifact] = Nil
    override def messageArtifacts: Seq[MessagesArtifact] = Nil
    override def errors: Int = 1
  }

  def apply(artifacts: Seq[Artifact], modifiedFiles: Option[FileDiff]): CompletedBuildResult =
    CompletedBuildResult(artifacts, modifiedFiles)
  def apply(t: Throwable): AbortedBuildResult = AbortedBuildResult(t)
}

/**
 * Core class for executing builds of multiple scopes.
 */
trait Builder {
  @async def build(
      scopes: Set[ScopeId],
      postBuilder: PostBuilder = PostBuilder.zero,
      modifiedFiles: Option[FileDiff] = None
  ): BuildResult
}

class StandardBuilder(
    val factory: ScopedCompilationFactory,
    scopeFilter: ScopeFilter = NoFilter,
    onBuildStart: Option[AsyncFunction0[Unit]] = None,
    onBuildEnd: Option[AsyncFunction0[Unit]] = None,
    backgroundBuilder: Option[BackgroundBuilder] = None,
    defaultPostBuilder: PostBuilder = PostBuilder.zero,
    messageReporter: Option[MessageReporter] = None,
    assetUploader: Option[AssetUploader] = None,
    uploadSources: Boolean = true,
    minimalInstallScopes: Option[Set[ScopeId]] = None
) extends Builder {

  @async override final def build(
      scopes: Set[ScopeId],
      postBuilder: PostBuilder,
      modifiedFiles: Option[FileDiff]
  ): BuildResult =
    buildScopes(scopedCompilations(scopes), postBuilder, modifiedFiles)

  protected val log: Logger = getLogger(this)

  @node private def scopedCompilations(scopeIds: Set[ScopeId]): Seq[ScopedCompilation] = {
    val directScopes = scopeIds.apar.flatMap(factory.lookupScope)
    val scopes = if (scopeFilter == NoFilter) {
      directScopes
    } else {
      // if we've got a scope filter, we actually want to apply it to the transitive runtime scopes rather than
      // just the direct scopes, so resolve those first. Note that transitiveScopes is a Set, so duplicates will
      // automatically be removed.
      val transitiveScopes = directScopes.apar.flatMap { s =>
        s.allCompileDependencies.apar.flatMap(_.transitiveScopeDependencies) ++
          s.runtimeDependencies.transitiveScopeDependencies
      }
      (directScopes ++ transitiveScopes).filter(scopeFilter(_))
    }
    scopes.toIndexedSeq.sortBy(_.id.properPath)
  }

  @async final private def buildScopes(
      scopedCompilations: Seq[ScopedCompilation],
      extraPostBuilder: PostBuilder,
      modifiedFiles: Option[FileDiff]
  ): BuildResult = {
    val startTime = patch.MilliInstant.now
    val totalScopes = scopedCompilations.size
    ObtTrace.setStat(ObtStats.Scopes, totalScopes)

    log.info(Utils.LogSeparator)
    val scopesStr = scopedCompilations.map(_.id).mkString(", ")
    if (totalScopes == 1)
      log.info(s"STARTING BUILD for ${scopedCompilations.head.id} at $startTime")
    else {
      log.info(f"STARTING BUILD for $totalScopes%,d scopes at $startTime")
      log.debug(s"Scopes: $scopesStr")
    }
    ObtTrace.info(f"STARTING BUILD ($totalScopes%,d scopes): $scopesStr")

    val cs = EvaluationContext.cancelScope.childScope()
    val postBuilder = PostBuilder.merge(Seq(defaultPostBuilder, extraPostBuilder))

    val result = asyncResult(cs) {
      onBuildStart.foreach(_())

      val completedScopes = ConcurrentHashMap.newKeySet[ScopeId]()
      val completed = new AtomicInteger(0)

      val timingListener = new AggregateTimingObtTraceListener()
      val (_, allArtifacts) = ObtTrace.withExtraListener(timingListener) {
        val backgroundCS = cs.childScope()
        apar(
          runBackgroundProcesses(backgroundCS),
          ObtTrace.traceTask(RootScopeId, Build) {
            // no point trying to build stuff if there are global configuration errors
            if (factory.globalMessages.exists(_.hasErrors)) factory.globalMessages
            else {
              val compiledArtifacts = scopedCompilations.apar.flatMap { sc =>
                val scopeId = sc.id
                log.debug(s"[$scopeId] Generating all artifacts...")
                val artifacts = asyncResult(cs) {
                  val as = sc.allArtifacts

                  // In cases where we want to process artifacts eagerly (and in parallel), `postProcessScopeArtifacts`
                  // can be overridden by subclasses. Note that this method will only be called for scopes we've
                  // requested to build directly; any scopes referenced transitively will need to be processed by
                  // `postProcessTransitiveArtifacts` later. This isn't ideal, but we don't know ahead of time
                  // what all the transitive scopes are, so we can't easily process them before all
                  // compilations have been completed (or we risk reprocessing transitive scopes which are referenced
                  // by multiple direct scopes).
                  postBuilder.postProcessScopeArtifacts(scopeId, as.all)
                  as
                }.valueOrElse { e =>
                  val exceptionScopeId = e match {
                    case ce: CompilationException => ce.scopeId
                    case _                        => scopeId
                  }
                  throw new RuntimeException(s"Failed to build $exceptionScopeId", e)
                }

                // Make this a def so we can call it (and add to completedScopes) at the point of logging
                def progress = {
                  completedScopes.add(scopeId)
                  val scopesCompleted = completed.incrementAndGet()
                  val progressStr = s"completed $scopesCompleted/$totalScopes scopes"
                  ObtTrace.reportProgress(s"$progressStr...", scopesCompleted.toDouble / totalScopes)
                  s"($progressStr)"
                }
                // As well as being useful logging, inspecting `artifacts` ensures we don't log (and call `progress`)
                // until the artifacts are generated
                artifacts.scope match {
                  case Seq(artifact) => log.info(s"[$scopeId] 1 artifact generated $progress: $artifact")
                  case as            => log.info(s"[$scopeId] ${as.size} artifacts generated $progress")
                }
                val hasErrors = artifacts.all.exists(_.hasErrors)
                if (hasErrors) backgroundCS.cancel("compilation errors found")
                artifacts.all
              }.distinct

              val compiledScopeIds = Artifact.scopeIds(compiledArtifacts).toSet
              // Note: We deliberately exclude `compiledScopeIds` here since if we've explicitly built them
              // then we don't need to build their pathing jars again
              val bundleScopes = {
                val compiledPathingBundles = factory.scopeConfigSource.pathingBundles(compiledScopeIds)
                minimalInstallScopes match {
                  case Some(minimal) =>
                    // when --minimal, we only includes direct bundle=true modules artifacts in related pathing jars
                    // since we won't install transitive modules app scripts, install trans-bundle pathing jar here for
                    // runtime usages(/bin dir) doesn't make any sense
                    factory.scopeConfigSource.pathingBundles(minimal).intersect(compiledPathingBundles)
                  case None => compiledPathingBundles
                }
              } -- compiledScopeIds
              // search all bundle=true modules as additional build targets to generate bundle artifacts
              val bundleCompilations = bundleScopes.toSeq.apar.flatMap(factory.lookupScope)
              val bundleArtifacts = bundleCompilations.apar.flatMap { sc =>
                val scopeId = sc.id
                log.debug(s"[$scopeId] Generating bundle artifacts...")
                val artifacts = sc.bundlePathingArtifacts(compiledArtifacts)
                artifacts match {
                  case Seq(_) => log.debug(s"[$scopeId] 1 bundle artifact generated")
                  case _      => log.debug(s"[$scopeId] ${artifacts.size} bundle artifacts generated")
                }
                artifacts
              }.distinct

              validate(factory.globalMessages ++ compiledArtifacts ++ bundleArtifacts)
            }
          }
        )
      }

      val res = BuildResult(allArtifacts, modifiedFiles)
      val directScopes = scopedCompilations.map(_.id).toSet

      if (res.successful) {
        log.info(s"${allArtifacts.size} artifacts successfully generated")
        val transitiveScopes = Artifact.transitiveIds(directScopes, allArtifacts)
        ObtTrace.setStat(ObtStats.TransitiveScopes, transitiveScopes.size)
        val scalaArtifacts = allArtifacts.collect {
          case InternalArtifact(id, a) if id.tpe == ArtifactType.Scala => a
        }
        ObtTrace.setStat(ObtStats.ScalaArtifacts, scalaArtifacts.size)
        val javaArtifacts = allArtifacts.collect {
          case InternalArtifact(id, a) if id.tpe == ArtifactType.Java => a
        }
        ObtTrace.setStat(ObtStats.JavaArtifacts, javaArtifacts.size)

        if (transitiveScopes.nonEmpty) postBuilder.postProcessTransitiveArtifacts(transitiveScopes, allArtifacts)

        postBuilder.postProcessArtifacts(directScopes, allArtifacts, successful = true)
      } else {
        val failureScopes = res.failedScopes
        log.error(s"Artifact generation failed for ${failureScopes.size} scope(s): ${failureScopes.mkString(", ")}")
        Utils.FailureLog.error(Utils.LogSeparator)
        res.errorsByArtifact.foreach { case (artifact, errors) =>
          show(artifact.id, errors)
        }
        Utils.FailureLog.error(Utils.LogSeparator)
        postBuilder.postProcessArtifacts(directScopes, allArtifacts, successful = false)
      }

      val endTime = patch.MilliInstant.now
      val durationMillis = endTime.toEpochMilli - startTime.toEpochMilli
      val duration = Utils.durationString(durationMillis)
      val midfix = if (res.successful) "" else "with failures "
      val msg = s"BUILD COMPLETED ${midfix}in $duration at $endTime"

      if (res.successful) Utils.SuccessLog.info(msg) else Utils.FailureLog.error(msg)

      if (res.successful) logTimings(durationMillis, timingListener)
      logCacheDetails()
      DependencyDownloadTracker.summaryThenClean(DependencyDownloadTracker.downloadedHttpFiles.nonEmpty)(s =>
        log.debug(s))
      RemoteArtifactCacheTracker.summaryThenClean(RemoteArtifactCacheTracker.corruptedFiles.nonEmpty)(s => log.warn(s))
      messageReporter.foreach(_.writeReports(res))
      log.info(Utils.LogSeparator)
      onBuildEnd.foreach(_())
      res
    }

    val success = result.hasValue && result.value.successful
    // failures during completion are considered fatal
    postBuilder.complete(success)
    try postBuilder.save(success)
    catch { case NonFatal(e) => log.error("Failed to save state", e) }

    result.valueOrElse {
      case OsVersionMismatch((t, e)) =>
        val endTime = patch.MilliInstant.now
        val durationMillis = endTime.toEpochMilli - startTime.toEpochMilli
        val duration = Utils.durationString(durationMillis)
        Utils.WarningLog.warn(t.getMessage)
        Utils.WarningLog.warn(e.getMessage)
        Utils.WarningLog.warn(s"BUILD COMPLETED partially in $duration at $endTime")
        BuildResult(t)
      case t =>
        val endTime = patch.MilliInstant.now
        val durationMillis = endTime.toEpochMilli - startTime.toEpochMilli
        val duration = Utils.durationString(durationMillis)
        Utils.FailureLog.error(s"BUILD COMPLETED with exception in $duration at $endTime", t)
        t match {
          case ce: CompilationException => show(ce.artifactId, ce.otherMessages)
          case _                        => // do nothing
        }
        messageReporter.foreach(_.errorReporter.writeErrorReport(t))
        throw t
    }
  }

  private def validate(allArtifacts: Seq[Artifact]): Seq[Artifact] = {
    val duplicateArtifacts = allArtifacts.groupBy(_.id).filter(_._2.size > 1)
    val scopedDuplicates = duplicateArtifacts.groupBy {
      case (InternalArtifactId(scopeId, _, _), _) =>
        scopeId
      case _ =>
        RootScopeId
    }
    val duplicateMessageArtifacts = scopedDuplicates
      .map { case (scopeId, dupes) =>
        val messages = dupes
          .map { case (id, as) =>
            CompilationMessage.error(s"Multiple (${as.size}) artifacts for key $id\n\t${as.mkString("\n\t")}")
          }
          .to(Seq)
        InMemoryMessagesArtifact(
          InternalArtifactId(scopeId, ArtifactType.DuplicateMessages, None),
          messages,
          Validation)
      }
      .to(Seq)

    allArtifacts ++ duplicateMessageArtifacts
  }

  private def show(id: InternalArtifactId, ms: Seq[CompilationMessage]): Unit = ms.foreach { m =>
    val pos = m.pos.fold("")(p => s"${p.filepath}:${p.startLine}") // Intellij-friendly format (for copy-paste)
    log.error(s"[$id] $pos: ${m.msg}")
  }

  @async private def runBackgroundProcesses(cs: CancellationScope): Unit =
    asyncResult(cs) {
      apar(
        backgroundBuilder.foreach(_.build(RootScopeId, BackgroundCommand, lastLogLines = 20)),
        if (uploadSources) assetUploader.foreach(_.scheduleUploadConfigFromSource())
      )
    }.valueOrElse {
      case ex: OptimusCancellationException =>
        log.info(s"Background execution cancelled because of ${ex.getMessage}")
      case ex => throw ex
    }

  private def logTimings(durationMillis: Long, timingListener: AggregateTimingObtTraceListener): Unit = {
    val timings = timingListener.categoryToTotalMillis
    log.debug(
      f"""Timings:
         |\tTotal: $durationMillis%,dms
         |\tTotal scala CPU time (after signatures ready): ${timings(Scala)}%,dms
         |\tTotal CPU time to signatures ready: ${timings(Signatures)}%,dms
         |\tTotal java CPU time: ${timings(Java)}%,dms
         |\tTotal queue time: ${timings(Queue)}%,dms""".stripMargin
    )
  }

  private def logCacheDetails(): Unit = {
    val caches = Caches.allCaches()
    val cacheDetails = caches.map { c =>
      val counters = c.getCountersSnapshot
      val evictions = EvictionReason.values().map(r => r -> counters.numEvictions(r)).collect {
        case (r, n) if n > 0 =>
          s"$r ($n)"
      }
      val evictionsStr = if (evictions.nonEmpty) s" [Evictions: ${evictions.mkString(", ")}]" else ""
      s"${c.getName}: ${c.getSizeIndicative}/${c.getMaxSize}$evictionsStr"
    }
    log.debug(s"Cache details: ${cacheDetails.mkString("\n\t", "\n\t", "")}")
  }
}
