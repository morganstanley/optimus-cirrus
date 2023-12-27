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
package optimus.buildtool.compilers.zinc.setup

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Optional
import java.util.UUID
import java.util.zip.Deflater

import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.AnalysisArtifactType
import optimus.buildtool.artifacts.Artifact.InternalArtifact
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.PathedArtifact
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.compilers.SyncCompiler
import optimus.buildtool.compilers.SyncCompiler.PathPair
import optimus.buildtool.compilers.Task
import optimus.buildtool.compilers.zinc.LookupTracker
import optimus.buildtool.compilers.zinc.ObtReadStamps
import optimus.buildtool.compilers.zinc.ObtZincFileConverter
import optimus.buildtool.compilers.zinc.SimpleVirtualFile
import optimus.buildtool.compilers.zinc.VirtualSourceFile
import optimus.buildtool.compilers.zinc.ZincCompilerFactory
import optimus.buildtool.compilers.zinc.ZincEntryLookup
import optimus.buildtool.compilers.zinc.ZincInterface
import optimus.buildtool.compilers.zinc.ZincUtils
import optimus.buildtool.compilers.zinc.mappers.MappingTrace
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.ConfigurableWarnings
import optimus.buildtool.files.JarAsset
import optimus.buildtool.trace.Java
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.utils.Utils
import optimus.platform._
import sbt.internal.inc.AnalyzingCompiler
import sbt.internal.inc.IncrementalCompilerImpl
import sbt.internal.inc.ZincInvalidationProfiler
import sbt.internal.inc.javac.JavaCompiler
import sbt.internal.inc.javac.{JavaTools => JavaToolsFactory}
import sbt.internal.inc.javac.Javadoc
import xsbti.Reporter
import xsbti.VirtualFile
import xsbti.VirtualFileRef
import xsbti.compile._

import scala.compat.java8.OptionConverters._

final case class Jars(
    signatureJar: Option[PathPair],
    signatureAnalysisJar: Option[PathPair],
    outputJar: PathPair,
    analysisJar: PathPair
)
object Jars {
  def apply(obtInputs: SyncCompiler.Inputs, analysisType: AnalysisArtifactType): Jars = {
    val uuid = UUID.randomUUID()
    val signatureJar = obtInputs.signatureOutPath.map(PathPair(uuid, _))
    val signatureAnalysisJar = signatureJar.map(_.forType(AT.SignatureAnalysis))
    val outputJar = PathPair(uuid, obtInputs.outPath)
    val analysisJar = outputJar.forType(analysisType)
    Jars(signatureJar, signatureAnalysisJar, outputJar, analysisJar)
  }
}

object ZincInputs {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}

class ZincInputs(
    settings: ZincCompilerFactory,
    scopeId: ScopeId,
    traceType: MessageTrace,
    obtInputs: SyncCompiler.Inputs,
    activeTask: => Task,
    lookupTracker: Option[LookupTracker]
) {
  import ZincInputs.log

  private val prefix = Utils.logPrefix(scopeId, traceType)

  val mischiefExtraScalacArgs =
    obtInputs.mischief.toSeq.flatMap(_.extraScalacArgs)

  def resolveWarnings: ConfigurableWarnings =
    (if (traceType == Java) obtInputs.javacConfig.warnings else obtInputs.scalacConfig.warnings).configured

  def create(
      incrementalCompiler: IncrementalCompilerImpl,
      sources: Seq[VirtualSourceFile],
      inputClasspath: Seq[PathedArtifact],
      previousSignatureAnalysisJar: Option[JarAsset],
      jars: Jars,
      analysisStore: AnalysisStore,
      analysisMappingTrace: MappingTrace,
      classFileManager: ClassFileManager,
      invalidationProfiler: ZincInvalidationProfiler,
      reporter: Reporter,
      signatureConsumer: Option[ObtSignatureConsumer]
  ): Inputs = {
    activeTask.trace.reportProgress(s"preparing ${traceType.name} inputs")

    val upstreamClassJars: Map[InternalArtifactId, PathedArtifact] = inputClasspath.collect {
      case InternalArtifact(id, a) => id -> a
    }.toSingleMap

    val upstreamAnalyses: Seq[(InternalArtifactId, AnalysisArtifact)] =
      obtInputs.inputArtifacts.collect { case InternalArtifact(id: InternalArtifactId, a: AnalysisArtifact) =>
        (id, a)
      }

    val classJarToAnalysis: Map[InternalArtifact, AnalysisArtifact] = upstreamAnalyses.flatMap {
      case (analysisId, analysis) =>
        val classTypes: Seq[AT] = analysisId.tpe match {
          // we can use signature analysis for all class/signature jar types
          case AT.SignatureAnalysis => Seq(AT.JavaAndScalaSignatures, AT.Scala, AT.Java)
          case AT.ScalaAnalysis     => Seq(AT.Scala)
          case AT.JavaAnalysis      => Seq(AT.Java)
          case t                    => throw new IllegalStateException(s"Unexpected analysis type: $t")
        }

        val classJars = classTypes.flatMap { tpe =>
          val classId = InternalArtifactId(analysisId.scopeId, tpe, None)
          upstreamClassJars.get(classId).map(InternalArtifact(classId, _))
        }

        if (classJars.isEmpty)
          throw new IllegalStateException(
            s"${analysisId.scopeId} for types [${classTypes.mkString(", ")}] found in analyses but not classpath"
          )
        classJars.map(a => a -> analysis)
    }.toSingleMap

    final case class ClasspathElement(path: Path, file: VirtualFile, artifact: Option[PathedArtifact])

    val classpathDetails =
      inputClasspath.map(a => ClasspathElement(a.path, SimpleVirtualFile(a.path), Some(a))) ++
        settings.coreClasspath.map(f => ClasspathElement(Paths.get(f.id), f, None))

    val fullClasspathMap: Map[Path, VirtualFile] =
      classpathDetails.groupBy(_.path).map { case (p, es) =>
        es match {
          case Seq(e) => p -> e.file
          case _ =>
            log.debug(
              s"Multiple artifacts for path $p: ${es.map(e => e.artifact.map(_.id).getOrElse(e.file)).mkString(", ")}"
            )
            p -> SimpleVirtualFile(p)
        }
      }

    val fileConverter = new ObtZincFileConverter(sources.map(s => (s: VirtualFileRef) -> s).toMap, fullClasspathMap)

    val classpath = classpathDetails.map(_.file)
    log.trace(s"${prefix}Input classpath:\n  ${classpath.mkString("\n  ")}")

    val incOptions = this.incOptions(
      analysisStore = analysisStore,
      analysisMappingTrace = analysisMappingTrace,
      fileConverter = fileConverter,
      classFileManager = classFileManager,
      invalidationProfiler = invalidationProfiler,
      sources = sources,
      classpath = classpath,
      outputJar = jars.outputJar,
      inputClasspath = inputClasspath,
      classJarToAnalysis = classJarToAnalysis,
      lookupTracker = lookupTracker
    )

    val progressReporter = this.progressReporter(
      activeTask,
      jars,
      previousSignatureAnalysisJar,
      signatureConsumer
    )

    val compilers = incrementalCompiler.compilers(javac, scalac)

    val incrementalSetup = incrementalCompiler.setup(
      lookup = lookup(classJarToAnalysis),
      skip = false,
      cacheFile = jars.analysisJar.tempPath.path,
      cache = CompilerCache.fresh,
      incOptions = incOptions,
      reporter = reporter,
      progress = Some(progressReporter),
      earlyAnalysisStore = signatureAnalysisStore(jars.signatureAnalysisJar),
      extra = Array()
    )

    val dirName = s"${NamingConventions.TEMP}${UUID.randomUUID()}_zinc_classes"
    val tmpDirPath = settings.buildDir.path.getParent.resolve("tmp").resolve(dirName)
    Files.createDirectories(tmpDirPath)

    val combinedScalacOptions =
      if (traceType == Java) Array.empty[String]
      else (scalacOptions(jars.signatureJar) ++ mischiefExtraScalacArgs).toArray

    val previousAnalysis = analysisStore.get.asScala match {
      case Some(a) =>
        log.debug(s"${prefix}Loaded previous analysis $a")
        PreviousResult.of(Optional.of(a.getAnalysis), Optional.of(a.getMiniSetup))
      case None =>
        log.debug(s"${prefix}Starting with empty analysis")
        PreviousResult.create(Optional.empty[CompileAnalysis](), Optional.empty[MiniSetup]())
    }

    incrementalCompiler.inputs(
      classpath = classpath.toArray,
      sources = sources.toArray,
      classesDirectory = jars.outputJar.tempPath.path,
      earlyJarPath = jars.signatureJar.map(_.tempPath.path),
      scalacOptions = combinedScalacOptions,
      javacOptions = javacOptions.toArray,
      maxErrors = Int.MaxValue,
      sourcePositionMappers = Array(),
      order = CompileOrder.Mixed,
      compilers = compilers,
      setup = incrementalSetup,
      pr = previousAnalysis,
      temporaryClassesDirectory = Optional.of(tmpDirPath),
      converter = fileConverter,
      stampReader = ObtReadStamps
    )
  }

  private def lookup(classJarToAnalysis: Map[InternalArtifact, AnalysisArtifact]): ZincEntryLookup = {
    val classpathToAnalysis: Map[VirtualFile, Path] = classJarToAnalysis.map { case (classJar, analysis) =>
      SimpleVirtualFile(classJar.artifact.path) -> analysis.path
    }

    ZincEntryLookup(classpathToAnalysis, settings.analysisCache)
  }

  private def incOptions(
      analysisStore: AnalysisStore,
      analysisMappingTrace: MappingTrace,
      fileConverter: ObtZincFileConverter,
      classFileManager: ClassFileManager,
      invalidationProfiler: ZincInvalidationProfiler,
      sources: Seq[VirtualSourceFile],
      classpath: Seq[VirtualFile],
      outputJar: PathPair,
      inputClasspath: Seq[PathedArtifact],
      classJarToAnalysis: Map[InternalArtifact, AnalysisArtifact],
      lookupTracker: Option[LookupTracker]
  ): IncOptions = {

    val previousAnalysis = analysisStore.get.asScala

    val pathToInternalClassJar = inputClasspath.collect { case InternalArtifact(id, a) =>
      a.path -> InternalArtifact(id, a)
    }.toSingleMap

    val externalHooks = new ObtExternalHooks(
      scopeId = scopeId,
      settings = settings,
      fileConverter = fileConverter,
      previousAnalysis = previousAnalysis,
      analysisMappingTrace = analysisMappingTrace,
      sources = sources,
      classpath = classpath,
      fingerprintHash = obtInputs.fingerprintHash,
      outputJar = outputJar,
      classType = ZincUtils.classType(traceType),
      pathToInternalClassJar = pathToInternalClassJar,
      classJarToAnalysis = classJarToAnalysis,
      classFileManager = classFileManager,
      invalidationProfiler = invalidationProfiler,
      invalidateOnly = obtInputs.mischief.flatMap(_.invalidateOnly),
      lookupTracker = lookupTracker
    )

    // Because Zinc does its own rewrites to -Ypickle-write, we can't use platform independent paths, and therefore
    // our mappers don't understand it. So just ignore it along with any mischievous arguments
    val ignoredOptions = Array("-Ypickle-write .*") ++ mischiefExtraScalacArgs.map(o => s"$o.*")

    settings.zincOptionMutator(
      IncOptions
        .create()
        .withEnabled(true)
        .withStoreApis(true)
        .withRecompileAllFraction(settings.zincRecompileAllFraction)
        .withExternalHooks(externalHooks)
        .withIgnoredScalacOptions(ignoredOptions)
        .withAllowMachinePath(false)
        .withUseCustomizedFileManager(true)
        .withPipelining(traceType != Java) // pipelining disables full java compilation
    )
  }

  private def progressReporter(
      activeTask: => Task,
      jars: Jars,
      previousSignatureAnalysisJar: Option[JarAsset],
      onSignaturesComplete: Option[() => Unit]
  ) = new ObtProgressReporter(
    activeTask = activeTask,
    jars = jars,
    previousSignatureAnalysisJar = previousSignatureAnalysisJar,
    onSignaturesComplete = onSignaturesComplete
  )

  private def signatureAnalysisStore(signatureAnalysisJar: Option[PathPair]) =
    signatureAnalysisJar.map { j =>
      new SignatureAnalysisStore(scopeId, settings, obtInputs.saveAnalysis, j.tempPath)
    }

  private def scalac: AnalyzingCompiler = {
    val scalaInstance = settings.getScalaInstance(settings.scalaClassPath.map(_.path.toFile))
    val interfaceJar = ZincInterface.getInterfaceJar(
      settings.interfaceDir,
      scalaInstance,
      settings.zincClassPathForInterfaceJar,
      settings.zincVersion,
      activeTask.trace.reportProgress("compiling zinc interface")
    )
    val classpathOptions = ClasspathOptionsUtil.javac( /*compiler =*/ false)
    val compilerInterfaceProvider = ZincCompilerUtil.constantBridgeProvider(scalaInstance, interfaceJar.path.toFile)
    new AnalyzingCompiler(
      scalaInstance,
      compilerInterfaceProvider,
      classpathOptions,
      _ => (),
      settings.classLoaderCaches.classLoaderCache
    )
  }

  private def javac: JavaTools = {
    def javaFail() = throw new RuntimeException("Could not find a local java compiler")
    JavaToolsFactory(JavaCompiler.local.getOrElse(javaFail()), Javadoc.local.getOrElse(javaFail()))
  }

  private def scalacOptions(signatureJar: Option[PathPair]): Seq[String] = {

    val signatureArgs = signatureJar
      .map { j =>
        // not using platform independent path because Zinc rewrites this path and // confuses it
        Seq("-Ypickle-java", "-Ypickle-write", j.tempPath.path.toString)
      }
      .getOrElse(Nil)

    val pluginPathArgs = obtInputs.pluginArtifacts.map { as =>
      as.map(_.pathString).distinct.mkString("-Xplugin:", File.pathSeparator, "")
    }

    val outlineArgs =
      if (obtInputs.outlineTypesOnly)
        "-Youtline" :: "-Ystop-after:signaturer" :: "-Ymacro-expand:none" :: Nil
      else Nil

    val cachedClasspathArgs = if (settings.cachePluginAndMacroClassLoaders) {
      val macroClasspath = obtInputs.inputArtifacts.distinct.collect {
        case c: ClassFileArtifact if c.containsOrUsedByMacros => c.file.pathString
      }
      "-Ycache-plugin-class-loader:always" :: "-Ycache-macro-class-loader:always" ::
        "-Ymacro-classpath" :: macroClasspath.mkString(File.pathSeparator) :: Nil
    } else Nil

    val miscParams = obtInputs.scalacConfig.resolvedOptions

    val profilerArgs = settings.scalacProfileDir match {
      case Some(profilerDir) =>
        val profileDestination = profilerDir.path.resolve(s"$scopeId.csv")
        Seq("-Yprofile-enabled", "-Yprofile-destination", profileDestination.toString)
      case _ => Seq.empty
    }

    // This stops Scalac from looking up symbols on the java.class.path (which is OBT's classpath). This is very
    // important, because without it we see symbols from OBT's version of codetree, not the version of codetree that
    // we are currently compiling. Note that the IsolatedScalaCompilerClient and Zinc's own classloader isolation
    // isn't sufficient to prevent that leakage. In particular this was causing problems with entityagent symbols.
    val mandatoryArgs = "-usejavacp:false" ::
      // since we hash and repack the jars, we don't want Scalac to waste time compressing them
      "-Yjar-compression-level" :: Deflater.NO_COMPRESSION.toString :: Nil

    val scalacArgs = List(
      signatureArgs,
      pluginPathArgs,
      outlineArgs,
      miscParams,
      // we pass the java -release parameter to scalac too so that scala code has the same restrictions on jdk api usage
      List("-release", obtInputs.javacConfig.release.toString),
      cachedClasspathArgs,
      mandatoryArgs,
      profilerArgs
    ).flatten
    log.debug(s"${prefix}Compiler args:\n  ${scalacArgs.mkString("\n  ")}")

    scalacArgs
  }

  // Generally java opts are to be set in workspace.obt.
  // These two are added automatically because they're needed for correctness and performance with zinc
  private def javacOptions: Seq[String] = Seq("-sourcepath", "", "-XDuseUnsharedTable=true") ++
    obtInputs.javacConfig.resolvedOptions
}
