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
import java.nio.file.Paths
import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.PathedArtifact
import optimus.buildtool.artifacts.SignatureArtifact
import optimus.buildtool.compilers.CompilationException
import optimus.buildtool.compilers.CompilerOutput
import optimus.buildtool.compilers.SignatureCompilerOutput
import optimus.buildtool.compilers.SyncCompiler
import optimus.buildtool.compilers.SyncCompiler.Inputs
import optimus.buildtool.compilers.Task
import optimus.buildtool.compilers.zinc.mappers.MappingTrace
import optimus.buildtool.compilers.zinc.reporter.ZincReporter
import optimus.buildtool.compilers.zinc.setup.ClassAnalysisStore
import optimus.buildtool.compilers.zinc.setup.Jars
import optimus.buildtool.compilers.zinc.setup.Messages
import optimus.buildtool.compilers.zinc.setup.ObtSignatureConsumer
import optimus.buildtool.compilers.zinc.setup.TrackingClassFileManager
import optimus.buildtool.compilers.zinc.setup.ZincInputs
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.Java
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtStats.SlowTypecheckDurationMs
import optimus.buildtool.trace.ObtStats.SlowTypechecks
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Scala
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.StackUtils
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.Utils.LocatedVirtualFile
import optimus.buildtool.utils
import optimus.buildtool.utils.stats.SourceCodeStats
import optimus.platform._
import optimus.tools.scalacplugins.entity.ThresholdProfiler
import optimus.tools.scalacplugins.entity.reporter.OptimusNonErrorMessages
import sbt.internal.inc.CompileFailed
import sbt.internal.inc.IncrementalCompilerImpl
import sbt.internal.inc.ZincInvalidationProfiler
import sbt.internal.prof.Zprof
import xsbti.compile.AnalysisContents

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap
import scala.compat.java8.OptionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

object ZincCompiler {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  // Free space below which compiler will consider a compile error to be potentially caused by a lack of
  // disk space (default: 500MB)
  lazy val MinFreeSpaceBytes: Long =
    sys.props.get("optimus.buildtool.minFreeSpaceBytes").map(_.toLong).getOrElse(500L * 1024 * 1024)

  // workaround for incompatible JNA native library version
  sys.props.put("sbt.io.jdktimestamps", "true")
}

class ZincCompiler(settings: ZincCompilerFactory, scopeId: ScopeId, traceType: MessageTrace) extends SyncCompiler {
  import ZincCompiler._

  private val prefix = Utils.logPrefix(scopeId, traceType)

  private val reporter = new ZincReporter(new ZincScopeLogger(scopeId, traceType), settings.bspServer)
  private val zincLogger = new ZincScopeLoggerWithErrorForwarding(reporter, scopeId, traceType)

  private val classType = ZincUtils.classType(traceType)
  private val messageType = ZincUtils.messageType(traceType)
  private val analysisType = ZincUtils.analysisType(traceType)

  override def compile(
      inputs: Inputs,
      signatureCompilerOutputConsumer: Option[Try[SignatureCompilerOutput] => Unit],
      activeTask: => Task
  ): CompilerOutput = {
    activeTask.trace.reportProgress(s"preparing ${traceType.name} compiler args")
    try {
      System.clearProperty("scala.home")

      val jars = Jars(inputs, analysisType)
      val sourceSizeBytes = inputs.sourceFiles.values.map(_.size).sum

      // We write to a uniquely named temporary jar and later atomically move it to the target name so that we
      // never leave the target file in a incomplete or incorrect state. We rely on this behavior in
      // ScopedCompilation because if a file with the target name is present, we assume is is good to use.
      log.info(s"${prefix}Starting compilation ($sourceSizeBytes bytes)")
      log.debug(s"${prefix}Output paths: ${jars.outputJar.tempPath.path} -> ${jars.outputJar.finalPath.path}")

      val (previousAnalysis, previousMessages, previousSignatureAnalysis) =
        setupPreviousArtifacts(jars, inputs.bestPreviousAnalysis.get)

      val incremental = previousAnalysis.isDefined

      val incrementalCompiler = new IncrementalCompilerImpl

      // HACK: Convert Paths to Strings and back again, to ensure we purely have paths from the default
      // filesystem. This avoids problems with (a) comparing native and zip-based paths and
      // (b) converting Paths to Files.
      val sources = inputs.sourceFiles.map { case (id, c) =>
        VirtualSourceFile(Paths.get(id.localRootToFilePath.pathString), c)
      }.toSeq

      val inputClasspath: Seq[PathedArtifact] = inputs.inputArtifacts
        .collect {
          case c: ClassFileArtifact => c
          case p: SignatureArtifact => p
        }
      log.trace(s"${prefix}Input classpath:\n  ${inputClasspath.mkString("\n  ")}")

      val analysisMappingTrace = new MappingTrace
      val classAnalysisStore = new ClassAnalysisStore(
        scopeId = scopeId,
        traceType = traceType,
        settings = settings,
        inputs = inputs,
        jars = jars,
        incremental = incremental,
        analysisMappingTrace = analysisMappingTrace
      )

      val classFileManager = new TrackingClassFileManager(classAnalysisStore.get.asScala)

      val invalidationProfiler = new ZincInvalidationProfiler

      val signatureConsumer = for {
        sigJar <- jars.signatureJar
        sigAnalysisJar <- jars.signatureAnalysisJar
        consumer <- signatureCompilerOutputConsumer
      } yield new ObtSignatureConsumer(
        scopeId = scopeId,
        traceType = traceType,
        activeTask = activeTask,
        settings = settings,
        inputs = inputs,
        incremental = incremental,
        signatureJar = sigJar,
        signatureAnalysisJar = sigAnalysisJar,
        signatureCompilerOutputConsumer = consumer
      )
      val lookupTracker = if (settings.zincTrackLookups) Some(new LookupTracker) else None
      val zincInputs = new ZincInputs(settings, scopeId, traceType, inputs, activeTask, lookupTracker)
      val compilationReporter = reporter.configure(zincInputs.resolveWarnings, inputs.mischief)

      val compilerInputs = zincInputs.create(
        incrementalCompiler = incrementalCompiler,
        sources = sources,
        inputClasspath = inputClasspath,
        previousSignatureAnalysisJar = previousSignatureAnalysis,
        jars = jars,
        analysisStore = classAnalysisStore,
        analysisMappingTrace = analysisMappingTrace,
        classFileManager = classFileManager,
        invalidationProfiler = invalidationProfiler,
        reporter = compilationReporter,
        signatureConsumer = signatureConsumer
      )

      if (traceType == Java) activeTask.trace.reportProgress("java")
      else activeTask.trace.reportProgress("starting compiler")
      val compileResult = Try {
        val result = incrementalCompiler.compile(compilerInputs, zincLogger)

        if (result.hasModified) {
          val analysisContents = AnalysisContents.create(result.analysis(), result.setup())
          activeTask.trace.reportProgress(s"storing ${traceType.name} final analysis")
          classAnalysisStore.set(analysisContents)
        }
      }

      // should always have exactly one runProfile because the invalidationProfiler is either freshly created
      // or else is wrapping the underlying one - see ZincCompileSetup
      val profile = invalidationProfiler.toProfile
      settings.instrumentation.zincInvalidationCallback.foreach(c => c(scopeId, activeTask.category, profile))

      val cycles = {
        val runs = profile.getRunsList.asScala
        val run = runs.singleOrNone
        run.map(_.getCyclesList.asScala.toIndexedSeq).getOrElse {
          log.warn(s"Expected exactly one runProfile but got ${runs.size}")
          Nil
        }
      }

      logAndTraceProfileStats(
        compileResult.isSuccess,
        activeTask,
        inputs.sourceFiles,
        previousAnalysis,
        cycles,
        profile,
        reporter.messages
      )

      val newMessages = compileResult match {
        case Failure(e) =>
          val msgRoot = s"Failed to compile ${jars.outputJar.tempPath.pathString} with Zinc compiler"
          val msg = s"$prefix$msgRoot"
          log.debug(msg, e)
          val freeDiskSpaceBytes = Utils.freeSpaceBytes(jars.outputJar.tempPath)
          freeDiskSpaceBytes.foreach { b =>
            val msg = f"Free disk space on build drive: ${b.toDouble / 1024 / 1024}%.1fMB"
            log.debug(s"$prefix$msg")
          }
          val lowDiskSpace = freeDiskSpaceBytes.exists(_ < MinFreeSpaceBytes)
          // This is pretty horrible, but unfortunately there's no good way to tell for sure if a compilation failure
          // is RT or not. We do try though, to prevent non-RT failures being cached (either in memory or on disk).
          val probablyRt =
            !lowDiskSpace && !reporter.messages.exists(_.msg.contains("There is not enough space on the disk"))
          if ((e.isInstanceOf[CompileFailed] || e.getClass.getName == "xsbt.InterfaceCompileFailed") && probablyRt) {
            if (!reporter.messages.exists(_.isError))
              reporter.addMessage(CompilationMessage.error(s"$msg: ${StackUtils.oneLineStacktrace(e)}"))
            reporter.messages
          } else if (e.isInstanceOf[IllegalArgumentException]) {
            reporter.addMessage(CompilationMessage.error(e.getMessage))
            reporter.messages
          } else {
            if (lowDiskSpace) {
              freeDiskSpaceBytes.foreach { b =>
                val msg = f"Low disk space on build drive (${b.toDouble / 1024 / 1024}%.1fMB free)"
                log.warn(s"$prefix$msg")
                ObtTrace.warn(s"$prefix$msg")
              }
            }
            throw new CompilationException(
              scopeId,
              msgRoot,
              reporter.messages,
              e,
              activeTask.artifactType,
              activeTask.category
            )
          }
        case Success(r) =>
          log.debug(s"${prefix}Successfully compiled ${jars.outputJar.tempPath.pathString} with Zinc compiler: $r")

          val compiledClasses = classFileManager.generatedClasses
          val deletedClasses = classFileManager.deletedClasses
          if (compiledClasses.isEmpty && deletedClasses.nonEmpty) {
            // In this scenario incremental compiler didn't call scalac and there are removed files
            // so json files produced by entity-plugin need to be updated (we may need to remove some entries)
            val jsonUpdater = new IsolatedJsonUpdaterClient(compilationReporter, scopeId, traceType)
            jsonUpdater.deleteEntries(
              deletedClasses = deletedClasses,
              outputJar = jars.outputJar.tempPath,
              pluginArtifacts = inputs.pluginArtifacts,
              scalaJars = settings.scalaClassPath
            )
            settings.instrumentation.jsonUpdaterCallback.foreach(_(deletedClasses.map(_.clazz)))
          }

          reporter.messages
      }

      val messages = Messages.messages(newMessages, previousMessages, classFileManager.staleSources)
      activeTask.trace.publishMessages(messages)

      // Intellij needs to associate source file location with the location of the message artifact.  There's no
      // truly great way to transmit this information, so we normally extract it by examining the position of individual
      // messages.  However, if there are no messages, we still need to locate the empty message file, so.... we generate
      // a bogus message.  We do not however use "bogus" in any variable names - only in this comment.
      val locatedMessages: Seq[CompilationMessage] =
        if (messages.nonEmpty) messages
        else {
          val loc = compilerInputs.options.sources.collectFirst { case LocatedVirtualFile(pos) =>
            CompilationMessage(
              Some(pos),
              NamingConventions.ScopeLocator,
              CompilationMessage.Info,
              Some(OptimusNonErrorMessages.INTERNAL_COMPILE_INFO.id.sn.toString),
              isSuppressed = false,
              isNew = false
            )
          }
          Seq() ++ loc
        }

      val (internals, externals) = lookupTracker.map(_.messages(inputs)).getOrElse((Seq.empty, Seq.empty))

      // important not to move files if we were cancelled during compilation because it might be incomplete
      val cancelScope = EvaluationContext.cancelScope
      if (cancelScope.isCancelled) throw cancelScope.cancellationCause
      else {
        val compiledClasses = classFileManager.generatedClasses
        // if nothing was compiled, the original signatures are still valid, and the signature callback won't have
        // been invoked by Zinc, so we should invoke it now
        if (compiledClasses.isEmpty && !messages.exists(_.isError))
          jars.signatureJar.foreach(j => if (j.tempPath.existsUnsafe) signatureConsumer.foreach(_()))

        val msgPath = Utils.outputPathForType(inputs.outPath, messageType)
        // Watched via `outputVersions` in `AsyncScalaCompiler.output`/`AsyncScalaCompiler.signatureOutput`
        val messagesArtifact =
          CompilerMessagesArtifact.unwatched(
            InternalArtifactId(scopeId, messageType, None),
            msgPath.asJson,
            locatedMessages,
            internals,
            externals,
            traceType,
            incremental
          )
        messagesArtifact.storeJson()

        if (inputs.outlineTypesOnly) {
          val output = CompilerOutput(None, messagesArtifact, None)
          log.info(s"${prefix}Completing outline compilation")
          log.debug(s"${prefix}Outline output: $output")
          output
        } else if (messagesArtifact.hasErrors) {
          val output = CompilerOutput(None, messagesArtifact, None)
          Utils.FailureLog.warn(s"${prefix}Completing compilation with errors")
          log.debug(s"${prefix}Errors: $output")
          output
        } else {
          activeTask.trace.reportProgress(s"storing ${traceType.name} classes")
          // It's possible that compilation doesn't end up writing a jar (for example, if the only java
          // files are package-info.java files then no content will be created). In that case, we create an empty jar
          // instead.
          if (!jars.outputJar.tempPath.existsUnsafe)
            utils.Jars.withJar(jars.outputJar.tempPath, create = true)(_ => ())
          val classes = {
            // note that we don't compress, because this jar is likely to later get passed in for another Zinc compilation
            // and then come back here again (slightly modified), and this repeated rewriting process is far more
            // efficient on non-compressed jars. We do compress in InstallingBuilder
            utils.Jars.stampJarWithConsistentHash(jars.outputJar.tempPath, compress = false)
            jars.outputJar.moveTempToFinal()
            // Watched via `outputVersions` in `AsyncScalaCompiler.output`/`AsyncScalaCompiler.signatureOutput` or
            // `doCompilation(...).watchForDeletion()` in `AsyncJavaCompiler.output`
            InternalClassFileArtifact.unwatched(
              InternalArtifactId(scopeId, classType, None),
              inputs.outPath,
              Hashing.hashFileOrDirectoryContent(inputs.outPath),
              incremental = incremental,
              containsPlugin = inputs.containsPlugin,
              containsOrUsedByMacros = inputs.containsMacros
            )
          }
          val analysis = if (inputs.saveAnalysis) {
            // don't stamp with content hash, since we don't ever use analysis artifacts as part of a fingerprint
            jars.analysisJar.moveTempToFinal()
            // Watched via `outputVersions` in `AsyncScalaCompiler.output`/`AsyncScalaCompiler.signatureOutput` or
            // `doCompilation(...).watchForDeletion()` in `AsyncJavaCompiler.output`
            Some(
              AnalysisArtifact.unwatched(
                InternalArtifactId(scopeId, analysisType, None),
                jars.analysisJar.finalPath,
                incremental
              )
            )
          } else None
          val output = CompilerOutput(Some(classes), messagesArtifact, analysis)
          log.info(s"${prefix}Completing compilation")
          log.debug(s"${prefix}Output: $output")
          output
        }
      }
    } catch {
      case e: CompilationException => throw e
      case NonFatal(e) =>
        throw new CompilationException(
          scopeId,
          s"Compilation failed for $scopeId",
          Nil,
          e,
          activeTask.artifactType,
          activeTask.category
        )
    }
  }

  def cancel(): Unit = zincLogger.cancel()

  private def setupPreviousArtifacts(
      jars: Jars,
      previousAnalysis: Option[AnalysisArtifact]
  ): (Option[AnalysisArtifact], Option[FileAsset], Option[JarAsset]) = previousAnalysis match {
    case Some(prevAnalysis) if prevAnalysis.existsUnsafe =>
      def logUsing(): Unit =
        log.debug(
          s"${prefix}Using previous (${Files.getLastModifiedTime(prevAnalysis.path)}) analysis file: ${prevAnalysis.pathString}"
        )

      val previousJars =
        settings.analysisCache.previousJars(settings.buildDir, classType, messageType, prevAnalysis.analysisFile)
      val (previousMessages, previousSignatureAnalysis) = previousJars match {
        case PreviousJars(Right(prevClassJar), Right(prevMessages), Right(prevSigJar), Right(prevSigAnalysis))
            if jars.signatureAnalysisJar.isDefined =>
          AssetUtils.atomicallyCopy(prevClassJar, jars.outputJar.tempPath)
          jars.signatureJar.foreach(j => AssetUtils.atomicallyCopy(prevSigJar, j.tempPath))
          AssetUtils.atomicallyCopy(prevAnalysis.analysisFile, jars.analysisJar.tempPath)
          logUsing()
          (Some(prevMessages), Some(prevSigAnalysis))
        case PreviousJars(Right(prevClassJar), Right(prevMessages), _, _) =>
          AssetUtils.atomicallyCopy(prevClassJar, jars.outputJar.tempPath)
          AssetUtils.atomicallyCopy(prevAnalysis.analysisFile, jars.analysisJar.tempPath)
          logUsing()
          (Some(prevMessages), None)
        case prevJars =>
          log.warn(
            s"${prefix}Not using previous analysis ${prevAnalysis.pathString} due to missing companion artifacts: ${prevJars
                .missing(jars.signatureAnalysisJar.isDefined)}"
          )
          (None, None)
      }
      (Some(prevAnalysis), previousMessages, previousSignatureAnalysis)
    case Some(prevAnalysis) =>
      log.warn(s"${prefix}Previous analysis artifact missing: ${prevAnalysis.pathString}")
      (None, None, None)
    case _ =>
      log.debug(s"${prefix}No previous analysis defined")
      (None, None, None)
  }

  private def logAndTraceProfileStats(
      success: Boolean,
      activeTask: Task,
      sources: SortedMap[SourceUnitId, HashedContent],
      previousAnalysis: Option[AnalysisArtifact],
      cycles: Seq[Zprof.CycleInvalidation],
      profile: Zprof.Profile,
      messages: Seq[CompilationMessage]
  ): Unit = {
    if (cycles.isEmpty) {
      if (success) { // Cycles are often empty if the build failed
        val msg = s"${prefix}No source invalidations"
        log.info(msg)
        ObtTrace.info(msg)
      }
    } else {
      def filter(files: Iterable[String]): Iterable[String] =
        if (traceType == Scala) files.filter(_.endsWith("scala")) else files.filter(_.endsWith("java"))

      val filteredSources = filter(sources.keySet.map(_.sourceFolderToFilePath.pathString))
      val totalSize = filteredSources.size
      val analysisStr = if (previousAnalysis.isDefined) "" else " (non-incremental)"

      cycles.reverse.zipWithIndex.foreach { case (c, i) =>
        val allInvalidatedSources = c.getInvalidatedSourcesList.asScala.map(profile.getStringTable(_))
        val invalidatedSources = filter(allInvalidatedSources)
        val msg: String = if (invalidatedSources.isEmpty) {
          s"Cycle ${i + 1}, No ${traceType.name} invalidations"
        } else {
          val srcst = invalidatedSources.take(10).map(Paths.get(_).getFileName.toString)
          val ddd =
            if (invalidatedSources.size > srcst.size) s"... ${invalidatedSources.size - srcst.size} more"
            else ""
          s"Cycle ${i + 1}, ${invalidatedSources.size}/$totalSize ${traceType.name} invalidations$analysisStr: ${srcst.toSeq.sorted
              .mkString("", ", ", ddd)}"
        }
        log.info(s"$prefix$msg")
        ObtTrace.info(s"$prefix$msg")
      }
    }

    val slowTypecheckDurations = republishSlowCompilationWarnings(messages)

    val tracer = activeTask.trace
    if (tracer.supportsStats) {
      tracer.addToStat(SlowTypechecks, slowTypecheckDurations.size)
      tracer.addToStat(SlowTypecheckDurationMs, (slowTypecheckDurations.sum * 1000).toInt)
      tracer.setStat(ObtStats.Compilations, 1)
      tracer.setStat(ObtStats.Cycles, cycles.size)
      tracer.setStat(ObtStats.SourceFiles, sources.size)
      tracer.setStat(ObtStats.InvalidatedSourceFiles, cycles.map(_.getInvalidatedSourcesCount).sum)
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
      // warnings/errors are captured as part of trace completion
      tracer.setStat(ObtStats.Info, messages.count(_.severity == CompilationMessage.Info))
    }
  }

  /**
   * Collect slow typecheck durations. Republishes slow compilations to WARNING as a side-effect, so that users can
   * clearly see them.
   */
  private def republishSlowCompilationWarnings(messages: Seq[CompilationMessage]): Seq[Double] = {
    messages.collect {
      case CompilationMessage(pos, msg @ ThresholdProfiler.MessageString(_, durationInSecs), _, _, _, _) =>
        val posStr = pos.map(p => s"${p.filepath.split("/").last}:${p.startLine}").getOrElse("?:?")
        val str = s"$prefix${msg} ($posStr)"
        log.warn(s"$prefix$msg")
        ObtTrace.warn(str)
        durationInSecs
    }
  }
}
