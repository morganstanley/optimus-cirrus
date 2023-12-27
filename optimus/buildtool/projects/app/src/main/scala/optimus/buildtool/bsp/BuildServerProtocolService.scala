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
package optimus.buildtool.bsp

import java.nio.file.Files
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField._
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import ch.epfl.scala.bsp4j._
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.app.IncrementalMode
import optimus.buildtool.builders.BuildResult
import optimus.buildtool.builders.StandardBuilder
import optimus.buildtool.builders.TrackedWorkspace
import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.config.HasScopeId
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.ScalaVersionConfig
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.files.LocalDirectoryFactory
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.resolvers.IvyResolver
import optimus.buildtool.rubbish.ArtifactRecency
import optimus.buildtool.scope.FingerprintHasher
import optimus.buildtool.trace.BreadcrumbTraceListener
import optimus.buildtool.trace.BuildTargetPythonOptions
import optimus.buildtool.trace.BuildTargetScalacOptions
import optimus.buildtool.trace.BuildTargetSources
import optimus.buildtool.trace.BuildTargets
import optimus.buildtool.trace.GitDiff
import optimus.buildtool.trace.MultiwayTraceListener
import optimus.buildtool.trace.ObtLogger
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.ObtTraceListener
import optimus.buildtool.trace.SyncTrace
import optimus.buildtool.trace.TraceFilter
import optimus.buildtool.utils.FileDiff
import optimus.buildtool.utils.GitLog
import optimus.buildtool.utils.Utils
import optimus.graph.CancellationScope
import optimus.graph.CircularReferenceException
import optimus.graph.NodeTask
import optimus.graph.PropertyNode
import optimus.platform._

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.util.Try

object BuildServerProtocolService {
  private val slog = getLogger(this)
  val IoLoggerName: String = classOf[BuildServerProtocolService].getName + ".io"
  private[this] val ioLog = getLogger(IoLoggerName)

  val ConfigHash = "config-hash"

  // default to true
  private val enableConfigHashCheck = sys.props.get("optimus.buildtool.enableConfigHashCheck").forall(_.toBoolean)
  // default to true
  private[bsp] val buildSparseScopesOnImport =
    sys.props.get("optimus.buildtool.buildSparseScopesOnImport").forall(_.toBoolean)

  private def in(method: String, args: => Any = ""): Unit = ioLog.debug(s"IN: $method [$args]")
  private def out(method: String, result: => Any = ""): Unit = ioLog.debug(s"OUT: $method [$result]")
  private[bsp] def send(method: String, args: => Any = ""): Unit = ioLog.debug(s"SEND: $method [$args]")

  object MethodHasScopeId {
    def unapply(nt: NodeTask): Option[ScopeId] =
      Try(nt.asInstanceOf[{ def id: ScopeId }].id).toOption
  }

  object EntityHasScopeId {
    def unapply(nt: NodeTask): Option[ScopeId] =
      Try(nt.asInstanceOf[PropertyNode[_]].entity.asInstanceOf[HasScopeId].id).toOption
  }

  @tailrec def unwrap(t: Throwable): Throwable = t match {
    case ce: CompletionException if ce.getCause != null => unwrap(ce.getCause)
    case _                                              => t
  }

  private[bsp] def translateException(message: String)(t: Throwable): (Throwable, Option[String]) = {
    slog.error(message, t)
    val unwrapped = unwrap(t)
    val exceptionMessage = unwrapped match {
      case cre: CircularReferenceException =>
        val ids = cre.cycle.asScala
          .collect {
            case MethodHasScopeId(id) => id
            case EntityHasScopeId(id) => id
          }
          .toIndexedSeq
          .reverse
        if (ids.nonEmpty) {
          val deduped = ids.head +: ids.sliding(2).collect { case Seq(a, b) if a != b => b }.toIndexedSeq
          Some(s"Circular dependency:\n\t${deduped.mkString(" ->\n\t")}")
        } else None
      case _ => None
    }
    (unwrapped, exceptionMessage)
  }
}

private[bsp] trait BSPServiceTrait extends BuildServer with ScalaBuildServer with PythonBuildServer

// (see the BSP spec here: https://build-server-protocol.github.io/docs/specification)
class BuildServerProtocolService(
    val workspaceName: String,
    val workspaceRoot: Directory,
    val workspaceSourceRoot: Directory,
    val buildDir: Directory,
    val outputDir: Directory,
    sendCrumbs: Boolean,
    val workspace: TrackedWorkspace,
    val builder: NodeFunction0[StandardBuilder],
    val installerFactory: NodeFunction1[Option[Directory], PostBuilder],
    val scalaVersionConfig: NodeFunction0[ScalaVersionConfig],
    val pythonEnabled: NodeFunction0[Boolean],
    val ivyResolvers: NodeFunction0[Seq[IvyResolver]],
    val directoryFactory: LocalDirectoryFactory,
    val dependencyCopier: DependencyCopier,
    val incrementalMode: IncrementalMode,
    val hasher: FingerprintHasher,
    gitLog: Option[GitLog],
    gitAwareMessages: Boolean,
    listeners: List[ObtTraceListener],
    traceFilter: Option[TraceFilter],
    recency: Option[ArtifactRecency],
    osVersion: String
) extends BSPServiceTrait {
  import BuildServerProtocolService._
  import TrackedWorkspace._

  private[bsp] object Ops {
    // The extra '/?' is a workaround for dodgy targets of the form
    // "obt://codetree1//optimus/platform/dal_core/main"
    private val TargetFormat = """obt://[^/]*//?([\w\-/]*)""".r

    implicit class BuildTargetOps(id: BuildTargetIdentifier) {
      def asPartialScope: Option[String] = id.getUri match {
        case TargetFormat(s) =>
          Some(s.replace('/', '.'))
        case s =>
          val msg = s"Unexpected target format: $s"
          slog.warn(msg)
          listener.warn(msg)
          None
      }

      // This may be unsafe, in that `id` may represent a partial scope rather than a complete one
      def asScopeId: Option[ScopeId] = asPartialScope.map(ScopeId.parse)
    }
  }

  private[bsp] val structureHasher = StructureHasher(hasher, builder, scalaVersionConfig, pythonEnabled, ivyResolvers)

  private[bsp] val structureBuilder =
    StructureBuilder(
      builder,
      scalaVersionConfig,
      pythonEnabled,
      directoryFactory,
      dependencyCopier,
      structureHasher,
      recency)

  private[bsp] var listening: Option[Future[Void]] = None

  private val cancellationScopes = ConcurrentHashMap.newKeySet[CancellationScope]()

  private var bspSessionListener: BSPSessionTraceListener = _

  private[bsp] val converter = new BspScopeIdsConverter(workspaceName)

  private val localFormatter =
    new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .appendValue(HOUR_OF_DAY, 2)
      .appendLiteral(':')
      .appendValue(MINUTE_OF_HOUR, 2)
      .optionalStart()
      .appendLiteral(':')
      .appendValue(SECOND_OF_MINUTE, 2)
      .optionalStart()
      .appendFraction(NANO_OF_SECOND, 3, 3, true)
      .appendPattern(" z")
      .toFormatter

  private[bsp] val sparseJarPathBuilder =
    InstallPathBuilder.dev(Utils.sparseJarDir(buildDir), Utils.SparseJarInstallVersion)

  // we track session initialization mostly so that we can die after a timeout if our client never connected to avoid
  // hanging around waiting forever
  @volatile private var sessionInitialized: Boolean = false
  def isSessionInitialized: Boolean = sessionInitialized

  private def newBuilder(
      cancellationScope: CancellationScope,
      listener: ObtTraceListener,
      bspListener: BSPTraceListener,
      rescan: Boolean = true
  ): CompletableFuture[BspBuilder] = {

    val scan =
      if (rescan) workspace.rescan(cancellationScope, listener)
      else CompletableFuture.completedFuture(())

    scan.thenApply(_ => new BspBuilder(this, cancellationScope, listener, bspListener))
  }

  private def newSyncer(
      cancellationScope: CancellationScope,
      listener: ObtTraceListener,
      bspListener: BSPTraceListener,
      rescan: Boolean
  ): CompletableFuture[BspSyncer] = {

    val scan =
      if (rescan) workspace.rescan(cancellationScope, listener)
      else CompletableFuture.completedFuture(())

    scan.thenApply(_ => new BspSyncer(this, cancellationScope, listener, bspListener, osVersion))
  }

  private[bsp] def cancelAll(tracer: ObtLogger): Unit = {
    val msg = "Cancelling all scopes"
    slog.info(msg)
    tracer.info(msg)
    cancellationScopes.forEach(_.cancel())
  }

  private[bsp] def cancelAll(tracer: ObtLogger, throwable: Throwable): Unit = {
    slog.warn(s"Cancelling all scopes", throwable)
    // Minimal logging in IntelliJ since we're expecting to output the full exception details separately anyway
    // Note that CircularReferenceException in particular has an extremely verbose toString
    tracer.warn(s"Cancelling all scopes due to ${unwrap(throwable).getClass.getName}")
    cancellationScopes.forEach(_.cancel(throwable))
  }

  override def onConnectWithClient(buildClient: BuildClient): Unit = {
    bspSessionListener = new BSPSessionTraceListener(buildClient, converter, workspaceSourceRoot, traceFilter)
  }

  override def buildInitialize(params: InitializeBuildParams): CompletableFuture[InitializeBuildResult] = {
    in("buildInitialize", params)
    val config = new ch.epfl.scala.bsp4j.BuildServerCapabilities()
    slog.info(s"Connecting to intellij")

    // We only have compilation and we can provide workspace structure for now ...
    config.setCompileProvider(new CompileProvider(List("java", "scala", "python").asJava))

    // And nothing else so far ...
    config.setDependencySourcesProvider(false)
    config.setBuildTargetChangedProvider(false)
    config.setInverseSourcesProvider(false)
    config.setResourcesProvider(false)
    config.setRunProvider(new RunProvider(Nil.asJava))

    val res = new InitializeBuildResult("OBT", params.getVersion, params.getBspVersion, config)
    val future = new CompletableFuture[InitializeBuildResult]()
    future.complete(res)
    out("buildInitialize", res)
    future
  }

  override def onBuildInitialized(): Unit = {
    in("onBuildInitialized")
    slog.info(s"Connected to Intellij!")
    sessionInitialized = true
  }

  override def buildShutdown(): CompletableFuture[Object] = {
    in("buildShutdown")
    bspSessionListener.info(s"Shutdown requested by client")
    bspSessionListener.cancelHeartbeat()
    cancelAll(bspSessionListener)
    CompletableFuture.completedFuture("")
  }

  override def buildTargetCompile(compileParams: CompileParams): CompletableFuture[CompileResult] = {
    in("buildTargetCompile", compileParams)
    val startTime = patch.MilliInstant.now
    val localStartTime = localFormatter.format(startTime.atZone(ZoneId.systemDefault))
    val startMsg = s"Starting BSP compilation at $localStartTime ($startTime)"
    bspSessionListener.info(startMsg)
    slog.debug(startMsg)

    val cancellationScope = CancellationScope.newScope()
    cancellationScopes.add(cancellationScope)

    val id = compileParams.getOriginId
    val (listener, bspListener) = buildListeners(id)

    val builder = newBuilder(cancellationScope, listener, bspListener)

    val modifiedFiles = this.modifiedFiles(cancellationScope, listener)

    val targets = compileParams.getTargets.asScala.toIndexedSeq
    slog.debug(s"Compilation targets: ${targets.map(_.getUri).mkString(", ")}")

    val args = Option(compileParams.getArguments)
      .map(_.asScala.toIndexedSeq.grouped(2).map { case Seq(k, v) => (k, v) }.toMap)
      .getOrElse(Map.empty)
    slog.debug(s"Compilation args: [${args.map { case (k, v) => s"$k=$v" }.mkString(", ")}]")
    val incremental = args.get("incremental").forall(_.toBoolean) // default to true
    val installDir = args.get("installDir").map(s => Directory(workspaceRoot.path.getFileSystem.getPath(s)))

    val buildResult: CompletableFuture[BuildResult] = combine(builder, modifiedFiles) { (b, filesAndLines) =>
      b.compile(id, targets, incremental, installDir, filesAndLines)
    }

    val compileResult: CompletableFuture[CompileResult] = buildResult.thenApply { br =>
      val completionTime = patch.MilliInstant.now
      val localCompletionTime = localFormatter.format(completionTime.atZone(ZoneId.systemDefault))
      val resStr =
        if (br.errors > 1) s"failed with ${br.errors} errors"
        else if (br.errors == 1) "failed with 1 error"
        else "succeeded"
      val durationMillis = completionTime.toEpochMilli - startTime.toEpochMilli
      val durationStr = Utils.durationString(durationMillis)
      bspSessionListener.info(s"Compilation $resStr in $durationStr at $localCompletionTime ($completionTime)")
      slog.debug(
        s"Build Statistics: ${StaticConfig.string("splunkSearchPath")}?q=search%20payload.obtBuildId%3D$id"
      )
      val status = if (br.errors > 0) StatusCode.ERROR else StatusCode.OK

      val compileResult = new CompileResult(status)
      compileResult.setOriginId(compileParams.getOriginId)
      out("buildTargetCompile", compileResult)
      compileResult
    }

    builder
      .thenCompose(notifyOnChangedHash(_, args, compileResult, bspListener))
      .whenComplete((_, _) => cancellationScopes.remove(cancellationScope))

    compileResult
  }

  private def notifyOnChangedHash(
      builder: BspBuilder,
      args: Map[String, String],
      compilation: CompletableFuture[_],
      bspListener: BSPTraceListener
  ): CompletableFuture[Unit] = {
    val fut = if (enableConfigHashCheck) {
      args.get(BuildServerProtocolService.ConfigHash).filter(_ != "unknown").map { previousHash =>
        val hash: CompletableFuture[String] = builder.configHash

        // Don't notify that workspace refresh is needed until both hash calculation and
        // compilation have completed normally
        compilation.thenCombine[String, Unit](
          hash,
          (_, latestHash) => {
            if (previousHash != latestHash) {
              slog.warn(s"Config hash has changed ($previousHash -> $latestHash). Workspace refresh needed.")
              bspListener.warn(
                "Workspace structure has changed: use 'Stratosphere' > 'Import Workspace' to update it."
              )
              bspListener.buildTargetsChanged()
            }
          }
        )
      }
    } else None
    fut.getOrElse(CompletableFuture.completedFuture(()))
  }

  override def onBuildExit(): Unit = {
    in("onBuildExit")
    bspSessionListener.info("Exit requested by client")
    bspSessionListener.info("Shutting down service")
    listening.foreach(_.cancel(true))
  }

  override def workspaceBuildTargets(): CompletableFuture[WorkspaceBuildTargetsResult] = {
    in("workspaceBuildTargets")
    val startTime = patch.MilliInstant.now
    bspSessionListener.info(s"Starting workspace refresh at $startTime")

    val cancellationScope = CancellationScope.newScope()
    cancellationScopes.add(cancellationScope)

    val (listener, bspListener, breadcrumbListener) = syncListeners(BuildTargets)
    val syncer = newSyncer(cancellationScope, listener, bspListener, rescan = true)

    syncer
      .thenCompose[Seq[BuildTarget]](
        _.buildTargets(buildSparseScopes = BuildServerProtocolService.buildSparseScopesOnImport))
      .thenApply[WorkspaceBuildTargetsResult] { targets =>
        listener.addToStat(ObtStats.Scopes, targets.size)
        val msg = f"Importing ${targets.size}%,d build targets"
        slog.info(msg)
        listener.info(msg)
        breadcrumbListener.foreach(_.publishStats())

        val res = new WorkspaceBuildTargetsResult(targets.asJava)
        out("workspaceBuildTargets", res)
        res
      }
      .whenComplete((_, _) => cancellationScopes.remove(cancellationScope))
  }

  override def buildTargetSources(params: SourcesParams): CompletableFuture[SourcesResult] = {
    in("buildTargetSources", params)

    val cancellationScope = CancellationScope.newScope()
    cancellationScopes.add(cancellationScope)

    val (listener, bspListener, breadcrumbListener) = syncListeners(BuildTargetSources)
    listener.addToStat(ObtStats.Scopes, params.getTargets.size)
    val syncer = newSyncer(cancellationScope, listener, bspListener, rescan = false)

    syncer
      .thenCompose[Seq[SourcesItem]](
        _.sources(params.getTargets.asScala.toIndexedSeq)
      )
      .thenApply[SourcesResult] { items =>
        val sourceFolders = items.map(_.getSources.size).sum
        listener.addToStat(ObtStats.Sync.SourceFolders, sourceFolders)
        val msg = f"Importing $sourceFolders%,d source folders for ${items.size}%,d targets"
        slog.info(msg)
        listener.info(msg)
        breadcrumbListener.foreach(_.publishStats())

        val res = new SourcesResult(items.asJava)
        out("buildTargetSources", res)
        res
      }
      .whenComplete((_, _) => cancellationScopes.remove(cancellationScope))
  }

  override def buildTargetPythonOptions(params: PythonOptionsParams): CompletableFuture[PythonOptionsResult] = {
    in("buildTargetPythonOptions", params)

    val outputBase = buildDir.resolveDir("fake-outputs")
    if (!outputBase.exists) Files.createDirectories(outputBase.path)

    val cancellationScope = CancellationScope.newScope()
    cancellationScopes.add(cancellationScope)

    val (listener, bspListener, breadcrumbListener) = syncListeners(BuildTargetPythonOptions)
    listener.addToStat(ObtStats.PythonScopes, params.getTargets.size)
    val syncer = newSyncer(cancellationScope, listener, bspListener, rescan = false)

    syncer
      .thenCompose[Seq[PythonOptionsItem]](_.pythonOptions(params.getTargets.asScala.toIndexedSeq, outputBase))
      .thenApply[PythonOptionsResult] { items =>
        val msg = f"Importing python configurations for ${items.size}%,d targets"
        slog.info(msg)
        listener.info(msg)
        breadcrumbListener.foreach(_.publishStats())
        // TODO (OPTIMUS-61422): apply interpreter options

        val res = new PythonOptionsResult(items.asJava)
        out("buildTargetPythonOptions", res)
        res
      }
      .whenComplete((_, _) => cancellationScopes.remove(cancellationScope))
  }

  override def buildTargetScalacOptions(params: ScalacOptionsParams): CompletableFuture[ScalacOptionsResult] = {
    in("buildTargetScalacOptions", params)

    // We need to create empty directories for IntelliJ
    // TODO (OPTIMUS-31029): Use bsp to generate classpath
    val outputBase = buildDir.resolveDir("fake-outputs")
    if (!outputBase.exists) Files.createDirectories(outputBase.path)

    val cancellationScope = CancellationScope.newScope()
    cancellationScopes.add(cancellationScope)

    val (listener, bspListener, breadcrumbListener) = syncListeners(BuildTargetScalacOptions)
    listener.addToStat(ObtStats.ScalacScopes, params.getTargets.size)
    val syncer = newSyncer(cancellationScope, listener, bspListener, rescan = false)

    syncer
      .thenCompose[Seq[ScalacOptionsItem]](
        _.scalacOptions(params.getTargets.asScala.toIndexedSeq, outputBase)
      )
      .thenApply[ScalacOptionsResult] { items =>
        val dependencies = items.map(_.getClasspath.asScala.size).sum
        listener.addToStat(ObtStats.Sync.Dependencies, dependencies)
        val msg = f"Importing scalac configurations (with $dependencies%,d dependencies) for ${items.size}%,d targets"
        slog.info(msg)
        listener.info(msg)
        breadcrumbListener.foreach(_.publishStats())

        val res = new ScalacOptionsResult(items.asJava)
        out("buildTargetScalacOptions", res)
        res
      }
      .whenComplete((_, _) => cancellationScopes.remove(cancellationScope))
  }

  private def syncListeners(
      category: SyncTrace
  ): (ObtTraceListener, BSPTraceListener, Option[BreadcrumbTraceListener]) = {
    val bspListener = bspSessionListener.forSync()
    val allListeners = bspListener :: listeners
    val breadcrumbListener = BreadcrumbTraceListener.forSync(sendCrumbs, workspaceRoot, category)(allListeners)
    (MultiwayTraceListener(allListeners ++ breadcrumbListener), bspListener, breadcrumbListener)
  }

  private def buildListeners(buildId: String): (ObtTraceListener, BSPTraceListener) = {
    val bspListener = bspSessionListener.forBuildId(buildId)
    val allListeners = bspListener :: listeners
    val breadcrumbListener = BreadcrumbTraceListener.forBuild(sendCrumbs, workspaceRoot, buildId)(allListeners)
    (MultiwayTraceListener(allListeners ++ breadcrumbListener), bspListener)
  }

  private def modifiedFiles(
      cancellationScope: CancellationScope,
      listener: ObtTraceListener
  ): CompletableFuture[Option[FileDiff]] = gitLog match {
    case Some(gl) if gitAwareMessages =>
      workspace.run(cancellationScope, listener) {
        ObtTrace.traceTask(RootScopeId, GitDiff) {
          gl.modifiedFiles()
        }
      }
    case _ => TrackedWorkspace.completed(None)
  }

  // Not supported operations
  override def buildTargetCleanCache(cleanCacheParams: CleanCacheParams): CompletableFuture[CleanCacheResult] =
    unsupported()
  override def buildTargetDependencyModules(
      dependencyModulesParams: DependencyModulesParams): CompletableFuture[DependencyModulesResult] = unsupported()
  override def buildTargetDependencySources(
      params: DependencySourcesParams): CompletableFuture[DependencySourcesResult] =
    unsupported()
  override def buildTargetInverseSources(
      inverseSourcesParams: InverseSourcesParams): CompletableFuture[InverseSourcesResult] =
    unsupported()
  override def buildTargetOutputPaths(outputPathsParams: OutputPathsParams): CompletableFuture[OutputPathsResult] =
    unsupported()
  override def buildTargetResources(params: ResourcesParams): CompletableFuture[ResourcesResult] =
    unsupported()
  override def buildTargetRun(runParams: RunParams): CompletableFuture[RunResult] =
    unsupported()
  override def buildTargetScalaTestClasses(params: ScalaTestClassesParams): CompletableFuture[ScalaTestClassesResult] =
    unsupported()
  override def buildTargetScalaMainClasses(params: ScalaMainClassesParams): CompletableFuture[ScalaMainClassesResult] =
    unsupported()
  override def buildTargetTest(testParams: TestParams): CompletableFuture[TestResult] =
    unsupported()
  override def debugSessionStart(debugSessionParams: DebugSessionParams): CompletableFuture[DebugSessionAddress] =
    unsupported()
  // potentially could rescan workspace and recompute the workspace structure hash, if useful
  override def workspaceReload(): CompletableFuture[AnyRef] = unsupported()

  protected def listener: ObtLogger = if (ObtTrace.isConfigured) ObtTrace else bspSessionListener

  private def unsupported[A](): CompletableFuture[A] = throw new UnsupportedOperationException
}

class BspScopeIdsConverter(workspaceName: String) {
  def toURI(scopeId: ScopeId): String = scopeId match {
    case RootScopeId => s"obt://$workspaceName/"
    case s           => s"${toURI(RootScopeId)}${s.meta}/${s.bundle}/${s.module}/${s.tpe}"
  }
}
