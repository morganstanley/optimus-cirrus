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

import ch.epfl.scala.bsp4j._
import optimus.buildtool.artifacts._
import optimus.buildtool.builders.BuildResult
import optimus.buildtool.builders.postbuilders.PostBuilder
import optimus.buildtool.builders.postbuilders.extractors.PythonVenvExtractorPostBuilder
import optimus.buildtool.builders.postbuilders.sourcesync.GeneratedScalaSourceSync
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config._
import optimus.buildtool.files.Directory
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.Pathed
import optimus.buildtool.files.RelativePath
import optimus.buildtool.trace._
import optimus.buildtool.utils.EmptyFileDiff
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.Utils
import optimus.graph.CancellationScope
import optimus.platform._
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.util.Log
import org.eclipse.lsp4j.jsonrpc.ResponseErrorException
import org.eclipse.lsp4j.jsonrpc.messages.ResponseError
import org.eclipse.lsp4j.jsonrpc.messages.ResponseErrorCode

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.CompletableFuture
import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.mutable
import scala.jdk.CollectionConverters._
object BspSyncer {
  private final case class ScopeInfo(
      name: String,
      dir: RelativePath,
      id: ScopeId,
      resolvedScope: ResolvedScopeInformation)
}

class BspSyncer(
    service: BuildServerProtocolService,
    cancellationScope: CancellationScope,
    listener: ObtTraceListener,
    bspListener: BSPTraceListener,
    osVersion: String
) extends Log {
  import BspSyncer._
  import service.Ops._
  import service._

  private val isWindows = OsUtils.isWindows(osVersion)

  val pythonVenvExtractor = new PythonVenvExtractorPostBuilder(service.buildDir, service.pythonBspConfig)

  def buildTargets(buildSparseScopes: Boolean): CompletableFuture[Seq[BuildTarget]] = run[Seq[BuildTarget]] {
    def handleBuildFailure(result: BuildResult): Unit = {
      val failureScopes = result.artifacts.filter(_.hasErrors)
      bspListener.error(s"Build failed for ${failureScopes.size} scope(s): ${failureScopes.map(_.id).mkString(", ")}")

      val errorMsgs: Seq[String] = result.messageArtifacts.filter(_.hasErrors).flatMap { messageArtifact =>
        messageArtifact.messages.collect {
          case m if m.severity == Severity.Error =>
            val pos = m.pos.fold("")(p => s"${p.filepath}:${p.startLine}") // Intellij-friendly format (for copy-paste)
            s"[${messageArtifact.id}] $pos: ${m.msg}"
        }
      }
      errorMsgs.foreach(bspListener.error)

      throw new RuntimeException(s"Build failed for scopes: ${failureScopes.map(_.id).mkString("\n")}")
    }

    val structure = structureBuilder.structure

    val (sparseScopesToBuild, sparseInstallers) =
      if (buildSparseScopes)
        (structure.scopes.values.flatMap(_.sparseInternalCompileDependencies).toSet, Seq(installerFactory(None)))
      else (Set.empty[ScopeId], Nil)
    val (pythonScopesToBuild, pythonInstallers) =
      if (structure.pythonEnabled && structure.extractVenvs)
        (
          structure.scopes.filter { case (_, scopeInfo) => scopeInfo.config.pythonConfig.isDefined }.keySet,
          Seq(pythonVenvExtractor))
      else (Set.empty[ScopeId], Nil)

    val scopesToBuild = sparseScopesToBuild ++ pythonScopesToBuild
    val installers = sparseInstallers ++ pythonInstallers
    val pythonArtifactHashMap: Map[ScopeId, String] = if (scopesToBuild.nonEmpty) {
      // pass an empty diff here since we know that for sparse scopes we have no local changes
      listener.startBuild()
      val results = builder().build(scopesToBuild, PostBuilder.merge(installers), modifiedFiles = Some(EmptyFileDiff))
      listener.endBuild(results.successful)
      if (!results.successful) handleBuildFailure(results)

      results.artifacts.collect { case pythonArtifact: PythonArtifact =>
        pythonArtifact.scopeId -> pythonArtifact.inputsHash
      }.toMap
    } else Map.empty

    listener.traceTask(ScopeId.RootScopeId, BuildTargets) {
      Files.createDirectories(workspaceSourceRoot.path)
      val groupedByScopeRoot = structure.scopes.toIndexedSeq.groupBy { case (_, scope) =>
        scope.config.paths.configurationFile.parent
      }
      val normalizedScopes = groupedByScopeRoot.flatMap {
        case (root, Seq((id, resolvedScope))) =>
          Seq(ScopeInfo(id.module, root, id, resolvedScope))
        case (root, multiple) =>
          val module = multiple.map(_._1.copy(tpe = "")).distinct.singleOrNone
          val mainScopeId = module.flatMap(findMainScopeId(multiple.map(_._1), _))
          multiple.map {
            case (id, resolvedScope) if mainScopeId.contains(id) =>
              ScopeInfo(id.module, root, id, resolvedScope)
            case (id, resolvedScope) =>
              ScopeInfo(s"${id.module}.${id.tpe}", resolvedScope.config.paths.scopeRoot, id, resolvedScope)
          }
      }
      // guard against multiple scopes with the same module or module.tpe name
      val uniquelyNamedScopes = normalizedScopes
        .groupBy(_.name)
        .values
        .flatMap { matchingScopes =>
          if (matchingScopes.size == 1) matchingScopes
          else matchingScopes.map(s => s.copy(name = s.id.properPath))
        }
        .toIndexedSeq

      @tailrec def findRootName(candidate: String): String =
        if (uniquelyNamedScopes.exists(_.name == candidate)) findRootName(s"workspace.$candidate")
        else candidate

      // We need such synthetic project to make Intellij operations fast
      val emptyRootTarget = mkBuildTarget(
        ScopeId.RootScopeId,
        findRootName(workspaceName),
        workspaceSourceRoot,
        isTest = false,
        Nil,
        None,
        None,
        None,
        Map.empty,
        pythonEnabled = false,
        extractVenvs = false,
        Some(structure.structureHash)
      )

      emptyRootTarget +: uniquelyNamedScopes
        .map(
          asBuildTarget(
            _,
            Some(structure.scalaConfig),
            structure.pythonEnabled,
            structure.extractVenvs,
            pythonArtifactHashMap))
    }
  }.whenComplete((_, _) => bspListener.ensureDiagnosticsReported(Nil))

  def sources(targets: Seq[BuildTargetIdentifier]): CompletableFuture[Seq[SourcesItem]] = run {
    val structure = structureBuilder.structure

    listener.traceTask(ScopeId.RootScopeId, BuildTargetSources) {
      for {
        target <- targets
        scopeId <- target.asScopeId
      } yield
        if (scopeId == ScopeId.RootScopeId) new SourcesItem(target, Nil.asJava)
        else {
          def asSourceItem(resourceDir: Boolean, generatedDir: Boolean = false)(d: Directory) = {
            if (!d.exists) None
            else {
              val baseUri = d.path.toUri.toString
              val finalUri = if (resourceDir) s"$baseUri?resource=true" else baseUri
              Some(new SourceItem(finalUri, SourceItemKind.DIRECTORY, generatedDir))
            }
          }

          val resolvedScope = structure.scopes(scopeId)

          val generatedSrcDir =
            // don't create the generated-obt directory if we've got a sparse scope
            if (resolvedScope.config.generatorConfig.nonEmpty && resolvedScope.local) {
              val dir = GeneratedScalaSourceSync.generatedRoot(resolvedScope.config.paths.absScopeRoot)
              // create the directory to ensure that it's tagged as a generated source dir even if
              // no sources are there yet
              Files.createDirectories(dir.path)
              asSourceItem(resourceDir = false, generatedDir = true)(dir)
            } else None

          val srcDirs =
            resolvedScope.config.absSourceRoots.flatMap(asSourceItem(resourceDir = false)) ++
              resolvedScope.config.absWebSourceRoots.flatMap(asSourceItem(resourceDir = false)) ++
              resolvedScope.config.absElectronSourceRoots.flatMap(asSourceItem(resourceDir = false)) ++
              resolvedScope.config.absResourceRoots.flatMap(asSourceItem(resourceDir = true)) ++
              generatedSrcDir

          new SourcesItem(target, srcDirs.asJava)
        }
    }
  }.whenComplete((_, _) => bspListener.ensureDiagnosticsReported(Nil))

  def pythonOptions(targets: Seq[BuildTargetIdentifier]): CompletableFuture[Seq[PythonOptionsItem]] = {
    run {
      listener.traceTask(ScopeId.RootScopeId, BuildTargetPythonOptions) {
        for {
          target <- targets
          scopeId <- target.asScopeId
        } yield {
          if (scopeId == ScopeId.RootScopeId)
            new PythonOptionsItem(target, Nil.asJava)
          else {
            // TODO (OPTIMUS-61423): Implement the classpath logic from scalacOptions() for pythonpaths
            // only will matter for internal deps and maybe for depcopied external libs.

            val options = new PythonOptionsItem(
              target,
              // TODO (OPTIMUS-61422): replace Nil with interpreter options if we have any, from structure.resolvedScope
              Nil.asJava
            )
            options
          }
        }
      }
    }.whenComplete((_, _) => bspListener.ensureDiagnosticsReported(Nil))
  }
  def scalacOptions(targets: Seq[BuildTargetIdentifier]): CompletableFuture[Seq[ScalacOptionsItem]] = run {
    val structure = structureBuilder.structure

    listener.traceTask(ScopeId.RootScopeId, BuildTargetScalacOptions) {
      for {
        target <- targets
        scopeId <- target.asScopeId
      } yield {
        if (scopeId == ScopeId.RootScopeId)
          new ScalacOptionsItem(target, Nil.asJava, Nil.asJava, PathUtils.mappedUriString(buildDir.path))
        else {
          val resolvedScope = structure.scopes(scopeId)

          val internalSparseClasspath =
            resolvedScope.sparseInternalCompileDependencies.map { id =>
              val jar = sparseJarPathBuilder.libDir(id).resolveJar(NamingConventions.scopeOutputName(id))
              intellijFriendlyURI(jar, alwaysIncludeSources = true)
            }
          val externalClasspath = resolvedScope.externalCompileDependencies.collect {
            case classFile: ExternalClassFileArtifact => intellijFriendlyURI(classFile)
          }

          new ScalacOptionsItem(
            target,
            resolvedScope.config.scalacConfig.options.asJava,
            (internalSparseClasspath ++ externalClasspath).asJava,
            PathUtils.mappedUriString(buildDir.path)
          )
        }
      }
    }
  }.whenComplete((_, _) => bspListener.ensureDiagnosticsReported(Nil))

  private def mkBuildTarget(
      scopeId: ScopeId,
      displayName: String,
      root: Directory,
      isTest: Boolean = false,
      moduleDependencies: Seq[ScopeId] = Nil,
      scalaConfig: Option[ScalaVersionConfig],
      javaConfig: Option[JavacConfiguration],
      pythonConfig: Option[PythonConfiguration],
      pythonArtifactHashMap: Map[ScopeId, String],
      pythonEnabled: Boolean,
      extractVenvs: Boolean,
      configHash: Option[String] = None
  ): BuildTarget = {
    val deps = moduleDependencies.map(id => new BuildTargetIdentifier(converter.toURI(id)))
    val tags = if (isTest) Seq(BuildTargetTag.TEST) else Seq(BuildTargetTag.LIBRARY)
    val defaultLangs = Seq("scala", "java")
    val usingPython = pythonEnabled && pythonConfig.nonEmpty
    val langs = if (usingPython) defaultLangs ++ Seq("python") else defaultLangs
    val capabilities = new BuildTargetCapabilities( /*canCompile = */ false, /*canTest = */ false, /*canRun =*/ false)
    val buildId = new BuildTargetIdentifier(s"${converter.toURI(scopeId)}")
    val res = new BuildTarget(buildId, tags.asJava, langs.asJava, deps.asJava, capabilities)
    res.setBaseDirectory(intellijFriendlyURI(root))
    res.setDisplayName(displayName)

    (pythonConfig, scalaConfig, javaConfig) match {
      case (Some(pythonCfg), _, _) if pythonEnabled =>
        val interpreter: Path = if (extractVenvs) {
          val hash = pythonArtifactHashMap
            .getOrElse(scopeId, throw new RuntimeException(s"Unable to locate venv for scope ${scopeId}"))
          val venv: Path = pythonVenvExtractor
            .venvLocation(scopeId, hash)
            .path
          val binaryDir = if (OsUtils.isWindows) "Scripts" else "bin"
          venv.resolve(s"venv").resolve(binaryDir)
        } else {
          Paths.get(pythonCfg.python.binPath.getOrElse("MISSING_INTERPRETER"))
        }
        res.setDataKind(BuildTargetDataKind.PYTHON)
        res.setData(
          new PythonBuildTarget(
            /*version = */ pythonCfg.python.version,
            /*interpreter = */ PathUtils.mappedUriString(interpreter)
          )
        )
      case (_, Some(scalaCfg), _) =>
        res.setDataKind(BuildTargetDataKind.SCALA)
        val scalaTarget = new ScalaBuildTarget(
          "org.scala-lang",
          scalaCfg.scalaVersion.value,
          scalaCfg.scalaMajorVersion,
          ScalaPlatform.JVM,
          scalaCfg.scalaJars.map(intellijFriendlyURI(_, alwaysIncludeSources = false)).asJava
        )
        javaConfig.foreach(c => scalaTarget.setJvmBuildTarget(asJvmBuildTarget(c)))
        res.setData(scalaTarget)
      case (_, _, Some(javaCfg)) =>
        res.setDataKind(BuildTargetDataKind.JVM)
        res.setData(asJvmBuildTarget(javaCfg))
      case _ =>
        configHash.foreach { hash =>
          res.setDataKind(BuildServerProtocolService.ConfigHash)
          res.setData(hash)
        }
    }
    res
  }

  private def asJvmBuildTarget(javaConfig: JavacConfiguration): JvmBuildTarget = {
    val javaRelease = javaConfig.release
    // IntelliJ refers to Java as 1.x for x < 10 (see com.intellij.pom.java.LanguageLevel)
    val javaReleaseString = if (javaRelease < 10) s"1.$javaRelease" else javaRelease.toString
    val jdkUri = PathUtils.mappedUriString(javaConfig.jdkHome.path)
    val jvmBuildTarget = new JvmBuildTarget(jdkUri, javaReleaseString)
    jvmBuildTarget
  }
  private def asBuildTarget(
      scopeInfo: ScopeInfo,
      scalaConfig: Option[ScalaVersionConfig],
      pythonEnabled: Boolean,
      extractVenvs: Boolean,
      pythonArtifactsHashMap: Map[ScopeId, String]
  ): BuildTarget = {
    val scope = scopeInfo.resolvedScope.config
    val scalaCfg = if (scope.javaOnly) None else scalaConfig
    val isTestScope = scopeInfo.id.tpe.toLowerCase.contains("test")
    val root = scope.paths.workspaceSourceRoot.resolveDir(scopeInfo.dir)
    mkBuildTarget(
      scopeInfo.id,
      scopeInfo.name,
      root,
      isTest = isTestScope,
      scopeInfo.resolvedScope.localInternalCompileDependencies,
      scalaCfg,
      Some(scopeInfo.resolvedScope.config.javacConfig),
      scopeInfo.resolvedScope.config.pythonConfig,
      pythonArtifactsHashMap,
      pythonEnabled,
      extractVenvs,
      configHash = None
    )
  }

  @closuresEnterGraph
  private def run[A](task: => A): CompletableFuture[A] =
    workspace.run(cancellationScope, listener)(task).exceptionally { t =>
      val (unwrapped, message) = Utils.translateException(t)
      val error = new ResponseError(
        ResponseErrorCode.InternalError,
        message.getOrElse(unwrapped.toString),
        unwrapped
      )
      throw new ResponseErrorException(error)
    }

  private def intellijFriendlyURI(jar: JarAsset, alwaysIncludeSources: Boolean = false): String = {
    val coreUrl = PathUtils.mappedUriString(jar.path)
    val srcJar = {
      val afsSrc = jar.parent.resolveJar(jar.name.replace(".jar", IvySourceKey))
      if (afsSrc.exists) afsSrc else jar.parent.resolveJar(jar.name.replace(".jar", SourceKey))
    }
    val javadocJar = {
      val afsDoc = jar.parent.resolveJar(jar.name.replace(".jar", IvyJavaDocKey))
      if (afsDoc.exists) afsDoc else jar.parent.resolveJar(jar.name.replace(".jar", JavaDocKey))
    }
    val additions = mutable.Buffer[String]()
    if (alwaysIncludeSources || srcJar.exists) additions += s"src=${PathUtils.mappedUriString(srcJar.path)}"
    if (javadocJar.exists) additions += s"javadoc=${PathUtils.mappedUriString(javadocJar.path)}"
    if (additions.isEmpty) coreUrl else s"$coreUrl${additions.mkString("?", "&", "")}"
  }

  private def intellijFriendlyURI(d: Directory): String = PathUtils.mappedUriString(d.path)

  private def intellijFriendlyURI(e: ExternalClassFileArtifact): String = {
    val coreUrl = PathUtils.mappedUriString(e.path)
    val additions = Seq(e.source.map("src" -> _), e.javadoc.map("javadoc" -> _)).collect { case Some((tpe, a)) =>
      s"$tpe=${PathUtils.mappedUriString(a.path)}"
    }
    if (additions.isEmpty) coreUrl else s"$coreUrl${additions.mkString("?", "&", "")}"
  }

  private def vsCodeFriendlyPath(p: Pathed): String = {
    val pathStr = p.pathString
    if (isWindows && pathStr.startsWith(AfsNamingConventions.AfsRootStr))
      pathStr.replaceFirst(AfsNamingConventions.AfsRootStr, AfsNamingConventions.AfsRootMapping)
    else pathStr
  }

  private def findMainScopeId(ids: Seq[ScopeId], p: ScopeId): Option[ScopeId] =
    ids.find(_.isMain).orElse {
      if (p.module.endsWith("test")) ids.find(_.tpe == "test")
      else None
    }

}
