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

import java.nio.file.Paths
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.compilers.Task
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.Java
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtStats.SlowTypecheckDurationMs
import optimus.buildtool.trace.ObtStats.SlowTypechecks
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Scala
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.stats.SourceCodeStats
import optimus.platform._
// import optimus.tools.scalacplugins.entity.ThresholdProfiler
import sbt.internal.inc.MiniSetupUtil
import sbt.internal.prof.Zprof
import sbt.internal.prof.Zprof.ZincRun
import xsbti.compile.AnalysisContents
import xsbti.compile.CompileResult
import xsbti.compile.{Inputs => CompilerInputs}
import xsbti.compile.MiniSetup

import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

object ZincProfiler {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  // default to true
  lazy val WarnOnDiscardedAnalysis: Boolean =
    sys.props.get("optimus.buildtool.warnOnDiscardedAnalysis").forall(_.toBoolean)

  // these are copied from ZincInvalidationProfiler - it's unfortunate that we have to depend on these
  // but the only other alternative is to implement our own profiler
  private val NameChangeReason = "Standard API name change in modified class."
  private val AnnotationChangeReason = "API change due to annotation definition."
  private val PrivateTraitMembersReason = "API change due to existence of private trait members in modified class."
  private val MacroChangeReason = "API change due to macro definition."
}

class ZincProfiler(scopeId: ScopeId, traceType: MessageTrace, compilerInputs: CompilerInputs) {
  import ZincProfiler._

  private val prefix = Utils.logPrefix(scopeId, traceType)

  def hasNoSourceInvalidations(cycles: Seq[Zprof.CycleInvalidation], compileResult: Option[CompileResult]): Boolean =
    cycles.isEmpty && compileResult.isDefined // Cycles are often empty if the build failed

  def logAndTraceProfileStats(
      compileResult: Option[CompileResult],
      activeTask: Task,
      sources: SortedMap[SourceUnitId, HashedContent],
      previousAnalysis: Option[AnalysisContents],
      profile: Zprof.Profile,
      run: Option[ZincRun],
      cycles: Seq[Zprof.CycleInvalidation],
      messages: Seq[CompilationMessage]
  ): Unit = {
    def filter(files: Iterable[String]): Iterable[String] =
      if (traceType == Scala) files.filter(_.endsWith("scala")) else files.filter(_.endsWith("java"))

    val tracer = activeTask.trace
    def name(i: Int) = profile.getStringTable(i)

    val runStats = run.map { r =>
      val changes = r.getInitial.getChanges
      val added = filter(changes.getAddedList.asScala.map(name(_)))
      val modified = filter(changes.getModifiedList.asScala.map(name(_)))
      val removed = filter(changes.getRemovedList.asScala.map(name(_)))
      RunStats(
        r,
        added.size,
        modified.size,
        removed.size,
        r.getInitial.getBinaryDependenciesList.asScala.map(name(_)).toSet
      )
    }

    val (invalidatedSourcesByCycle, totalSources) = if (cycles.isEmpty) {
      (Nil, 0)
    } else {
      val filteredSources = filter(sources.keySet.map(_.sourceFolderToFilePath.pathString))
      val totalSize = filteredSources.size

      val invalidated = cycles.reverse.map { c =>
        val allInvalidatedSources = c.getInvalidatedSourcesList.asScala.map(name(_))
        filter(allInvalidatedSources)
      }
      (invalidated, totalSize)
    }

    val invalidatedSources = invalidatedSourcesByCycle.map(_.size).sum

    val slowTypecheckDurations = republishSlowCompilationWarnings(messages)

    val reason: Option[(ObtStats.CompilationReason, Option[String])] =
      if (cycles.nonEmpty) {
        tracer.setStat(ObtStats.Compilations, 1)
        tracer.setStat(ObtStats.Cycles, cycles.size)
        tracer.setStat(ObtStats.SourceFiles, totalSources)
        tracer.addToStat(ObtStats.InvalidatedSourceFiles, invalidatedSources)
        tracer.addToStat(SlowTypechecks, slowTypecheckDurations.size)
        tracer.addToStat(SlowTypecheckDurationMs, (slowTypecheckDurations.sum * 1000).toInt)

        tracer.setStat(
          ObtStats.LinesOfCode,
          SourceCodeStats.getStats(sources.values.view.map(_.utf8ContentAsString)).realCodeLineCount
        )
        val sourcesByPath = sources.map { case (id, c) => id.localRootToFilePath.pathString -> c }
        val compiledSources = for {
          cycle <- cycles
          sourceId <- cycle.getInitialSourcesList.asScala
          sourcePath = profile.getStringTable(sourceId)
          source <- sourcesByPath.get(sourcePath)
        } yield source
        tracer.setStat(
          ObtStats.CompiledLinesOfCode,
          SourceCodeStats.getStats(compiledSources.view.map(_.utf8ContentAsString)).realCodeLineCount
        )

        runStats.map { rs =>
          tracer.setStat(ObtStats.AddedSourceFiles, rs.added)
          tracer.setStat(ObtStats.ModifiedSourceFiles, rs.modified)
          tracer.setStat(ObtStats.RemovedSourceFiles, rs.removed)
          tracer.addToStat(ObtStats.ExternalDependencyChanges, rs.changedExternalDependencies)

          val (reason, desc) = compilationReason(compileResult, previousAnalysis, profile, invalidatedSources, rs)
          tracer.setStat(reason, 1)
          (reason, desc)
        }
      } else {
        tracer.setStat(ObtStats.ZincCacheHit, 1)
        None
      }

    // warnings/errors are captured as part of trace completion
    tracer.setStat(ObtStats.Info, messages.count(_.severity == CompilationMessage.Info))

    if (hasNoSourceInvalidations(cycles, compileResult)) {
      val msg = s"${prefix}No source invalidations"
      log.info(msg)
      ObtTrace.info(msg)
    } else {
      val analysisStr = reason match {
        case Some((r @ ObtStats.CompilationsDueToNoAnalysis, _)) => s" (${r.str})"
        case Some((r, Some(d)))                                  => s" (due to ${r.str} [$d])"
        case Some((r, None))                                     => s" (due to ${r.str})"
        case None                                                => ""
      }

      invalidatedSourcesByCycle.zipWithIndex.foreach { case (invalidatedSources, index) =>
        val msg: String = if (invalidatedSources.isEmpty) {
          s"Cycle ${index + 1}, No ${traceType.name} invalidations"
        } else {
          val srcst = invalidatedSources.take(10).map(Paths.get(_).getFileName.toString)
          val ddd =
            if (invalidatedSources.size > srcst.size) s"... ${invalidatedSources.size - srcst.size} more"
            else ""
          s"Cycle ${index + 1}, ${invalidatedSources.size}/$totalSources ${traceType.name} invalidations$analysisStr: ${srcst.toSeq.sorted
              .mkString("", ", ", ddd)}"
        }

        log.info(s"$prefix$msg")
        ObtTrace.info(s"$prefix$msg")
      }
    }

    // If we found a previous analysis but zinc still thinks every source file in the scope was added then
    // that suggests zinc didn't use the previous analysis at all. This could be due to bad zinc read/write mapping.
    if (reason.exists { case (r, _) => r == ObtStats.CompilationsDueToDiscardedAnalysis }) {
      val msg =
        s"${prefix}Incremental analysis was found by OBT but apparently discarded by zinc. This may be a bug - please contact the OBT team unless this is expected."

      if (WarnOnDiscardedAnalysis) {
        log.warn(msg)
        ObtTrace.warn(msg)
      } else log.debug(msg)
    }
  }

  private def compilationReason(
      compileResult: Option[CompileResult],
      previousAnalysis: Option[AnalysisContents],
      profile: Zprof.Profile,
      invalidatedSources: Int,
      rs: RunStats
  ): (ObtStats.CompilationReason, Option[String]) = {

    val discardReason = for {
      cr <- compileResult
      pa <- previousAnalysis
      ar <- checkAnalysis(cr, pa)
    } yield ar

    // Note: The external change checks are done in approximate order of specificity -
    // for example, zinc can be quite targeted when there's an upstream API change, but
    // has to be very conservative when something like a macro changes.
    discardReason.getOrElse {
      val externalChanges = rs.run.getInitial.getExternalChangesList.asScala
      val reason = {
        if (previousAnalysis.isEmpty)
          ObtStats.CompilationsDueToNoAnalysis
        else if (rs.changed > 0)
          ObtStats.CompilationsDueToSourceChanges
        else if (externalChanges.exists(_.getReason == NameChangeReason))
          ObtStats.CompilationsDueToUpstreamApiChanges
        else if (externalChanges.exists(_.getReason == AnnotationChangeReason))
          ObtStats.CompilationsDueToUpstreamAnnotationChanges
        else if (externalChanges.exists(_.getReason == PrivateTraitMembersReason))
          ObtStats.CompilationsDueToUpstreamTraitChanges
        else if (externalChanges.exists(_.getReason == MacroChangeReason))
          ObtStats.CompilationsDueToUpstreamMacroChanges
        else if (rs.changedExternalDependencies.nonEmpty)
          ObtStats.CompilationsDueToExternalDependencyChanges
        else if (invalidatedSources == 0 && traceType == Scala) {
          // If we get an OBT cache miss on a scope with java sources, we always have
          // to rerun signature compilation since our previous (scala) analysis doesn't
          // contain details of java signatures. Fortunately, these compilations are generally
          // pretty fast.
          ObtStats.CompilationsDueToNonIncrementalJavaSignatures
        } else {
          log.debug(s"${prefix}Compilation for unknown reason detected: $profile")
          ObtStats.CompilationsDueToUnknownReason
        }
      }
      (reason, None)
    }
  }

  // This logic is intended to mirror that in `IncrementalCompilerImpl.prevAnalysis` while providing more information
  // on which params are different
  private def checkAnalysis(
      compileResult: CompileResult,
      previousAnalysis: AnalysisContents
  ): Option[(ObtStats.CompilationReason, Option[String])] = {
    val equiv = {
      import MiniSetupUtil._
      val equivOpts =
        equivOpts0(equivScalacOptions(compilerInputs.setup.incrementalCompilerOptions.ignoredScalacOptions))
      equivCompileSetup(NullLogger, equivOpts)(equivCompilerVersion)
    }
    if (equiv.equiv(previousAnalysis.getMiniSetup, compileResult.getMiniSetup))
      None
    else if (compileResult.getMiniSetup.compilerVersion != previousAnalysis.getMiniSetup.compilerVersion)
      Some(ObtStats.CompilationsDueToChangedCompilerVersion, Some(compileResult.getMiniSetup.compilerVersion))
    else
      checkOptions(compileResult, previousAnalysis, Scala)
        .orElse(checkOptions(compileResult, previousAnalysis, Java))
        .orElse(Some((ObtStats.CompilationsDueToDiscardedAnalysis, None)))
  }

  private def checkOptions(
      compileResult: CompileResult,
      previousAnalysis: AnalysisContents,
      optionType: MessageTrace
  ): Option[(ObtStats.CompilationReason, Option[String])] = {
    def isParam(s: String) = !s.startsWith("-")
    def toMap(opts: Seq[String]): Map[String, Option[String]] = opts match {
      case opt :: param :: rest if isParam(param) =>
        if (opt != pickleWriteFlag) toMap(rest) + (opt -> Some(param))
        else toMap(rest)
      case opt :: rest => toMap(rest) + (opt -> None)
      case Nil         => Map.empty
    }

    def opts(miniSetup: MiniSetup) = {
      val o = if (optionType == Scala) miniSetup.options.scalacOptions else miniSetup.options.javacOptions
      toMap(o.to(Seq))
    }

    val previous = opts(previousAnalysis.getMiniSetup)
    val current = opts(compileResult.getMiniSetup)

    val onlyPrevious = previous -- current.keySet
    val onlyCurrent = current -- previous.keySet
    val different = current.filter { case (k, v) =>
      previous.get(k).exists(p => v != p)
    }

    if (onlyCurrent.exists { case (k, v) => k.startsWith(pluginFlag) && v.isEmpty })
      Some(ObtStats.CompilationsDueToPluginChanges, None)
    else if (onlyCurrent.nonEmpty)
      Some(ObtStats.CompilationsDueToAddedCompilerOptions, Some(s"${onlyCurrent.keys.to(Seq).sorted.mkString(", ")}"))
    else if (onlyPrevious.nonEmpty)
      Some(
        ObtStats.CompilationsDueToRemovedCompilerOptions,
        Some(s"${onlyPrevious.keys.to(Seq).sorted.mkString(", ")}"))
    else if (different.contains(macroFlag))
      Some(ObtStats.CompilationsDueToMacroClasspathChanges, None)
    else if (different.nonEmpty)
      Some(ObtStats.CompilationsDueToChangedCompilerOptions, Some(s"${different.keys.to(Seq).sorted.mkString(", ")}"))
    else None
  }

  /**
   * Collect slow typecheck durations. Republishes slow compilations to WARNING as a side-effect, so that users can
   * clearly see them.
   */
  private def republishSlowCompilationWarnings(messages: Seq[CompilationMessage]): Seq[Double] = {
    //TODO
    /* messages.collect {
      case CompilationMessage(pos, msg @ ThresholdProfiler.MessageString(_, durationInSecs), _, _, _, _, _) =>
        val posStr = pos.map(p => s"${p.filepath.split("/").last}:${p.startLine}").getOrElse("?:?")
        val str = s"$prefix$msg ($posStr)"
        log.warn(s"$prefix$msg")
        ObtTrace.warn(str)
        durationInSecs
    } */
    Seq.empty
  }
}
