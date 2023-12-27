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
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import ch.epfl.scala.bsp4j._
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.builders.postbuilders.sourcesync.GeneratedScalaSourceSync
import optimus.buildtool.config._
import optimus.buildtool.files.Directory
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.Pathed
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.JsonSupport
import optimus.buildtool.trace.BuildTargetPythonOptions
import optimus.buildtool.trace.BuildTargetScalacOptions
import optimus.buildtool.trace.BuildTargetSources
import optimus.buildtool.trace.BuildTargets
import optimus.buildtool.trace.ObtTraceListener
import optimus.buildtool.utils.EmptyFileDiff
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.Utils
import optimus.graph.CancellationScope
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform.util.Log
import optimus.platform._
import org.eclipse.lsp4j.jsonrpc.ResponseErrorException
import org.eclipse.lsp4j.jsonrpc.messages.ResponseError
import org.eclipse.lsp4j.jsonrpc.messages.ResponseErrorCode

import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.immutable.Seq
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
  import BuildServerProtocolService.translateException
  import service._
  import service.Ops._

  private val isWindows = OsUtils.isWindows(osVersion)

  def buildTargets(buildSparseScopes: Boolean): CompletableFuture[Seq[BuildTarget]] = run[Seq[BuildTarget]] {
    val structure = structureBuilder.structure

    val sparseScopes = structure.scopes.values.flatMap(_.sparseInternalCompileDependencies).toSet
    // pass an empty diff here since we know that for sparse scopes we have no local changes
    if (buildSparseScopes && sparseScopes.nonEmpty)
      builder().build(sparseScopes, installerFactory(None), modifiedFiles = Some(EmptyFileDiff))

    listener.traceTask(ScopeId.RootScopeId, BuildTargets) {
      writeVsCodeConfig(structure)

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
        pythonEnabled = false,
        Some(structure.structureHash)
      )

      emptyRootTarget +: uniquelyNamedScopes
        .map(asBuildTarget(_, Some(structure.scalaConfig), structure.pythonEnabled))
    }
  }.whenComplete((_, _) => bspListener.ensureDiagnosticsReported(Nil))

  private def writeVsCodeConfig(structure: WorkspaceStructure): Unit = {
    val workspaceFile = workspaceSourceRoot.resolveFile(s"$workspaceName.code-workspace")
    Files.createDirectories(workspaceFile.parent.path)
    Utils.atomicallyWrite(workspaceFile, replaceIfExists = true) { p =>
      val content = Map("folders" -> Seq(Map("path" -> ".")))
      val writer = JsonSupport.jsonWriter(content, prettyPrint = true)
      writer(p)
    }

    val devContainer = workspaceSourceRoot.resolveFile(".devcontainer/devcontainer.json")
    Files.createDirectories(devContainer.parent.path)
    Utils.atomicallyWrite(devContainer, replaceIfExists = true) { p =>
      val content = Map("extensions" -> Seq("ms-vscode.cpptools"))
      val writer = JsonSupport.jsonWriter(content, prettyPrint = true)
      writer(p)
    }

    val cppFile = workspaceSourceRoot.resolveFile(".vscode/c_cpp_properties.json")
    Files.createDirectories(cppFile.parent.path)
    Utils.atomicallyWrite(cppFile, replaceIfExists = true) { p =>
      val configs = for {
        (_, info) <- structure.scopes.to(Seq)
        cfg <- info.config.cppConfigs.find(_.osVersion == osVersion).to(Seq)
        buildCfg <- cfg.release.to(Seq) ++ cfg.debug.to(Seq)
      } yield buildCfg

      // No great way to pick a compiler for the whole workspace, so just choose the first one
      val compiler = configs.map(_.toolchain).find(_ != CppToolchain.NoToolchain).map(_.compiler.pathString)
      val includes = configs.flatMap(_.includes).map(d => vsCodeFriendlyPath(d)).distinct.sorted

      val osContent = if (isWindows) {
        Map(
          "name" -> "Win32",
          "defines" -> Seq("_DEBUG", "UNICODE", "_UNICODE"),
          "windowsSdkVersion" -> "10.0.18362.1",
          "compilerPath" -> compiler.getOrElse("cl.exe"),
          "intellisenseMode" -> "windows-msvc-x64"
        )
      } else {
        Map(
          "name" -> "Linux",
          "defines" -> Nil,
          "compilerPath" -> compiler.getOrElse("/usr/bin/gcc"),
          "intellisenseMode" -> "linux-gcc-x64"
        )
      }

      val content = Map(
        "configurations" -> Seq(
          osContent ++ Map(
            "includePath" -> ("${workspaceFolder}/**" +: includes),
            "cStandard" -> "c11",
            "cppStandard" -> "c++11"
          )
        ),
        "version" -> 4
      )
      val writer = JsonSupport.jsonWriter(content, prettyPrint = true)
      writer(p)
    }
  }

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

  def pythonOptions(
      targets: Seq[BuildTargetIdentifier],
      fakeOutputs: Directory): CompletableFuture[Seq[PythonOptionsItem]] = {
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

            // we need to create empty directories for IntelliJ
            val buildDir = fakeOutputs.resolveDir(scopeId.toString)
            if (!buildDir.exists) Files.createDirectory(buildDir.path)

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
  def scalacOptions(
      targets: Seq[BuildTargetIdentifier],
      fakeOutputs: Directory
  ): CompletableFuture[Seq[ScalacOptionsItem]] = run {
    val structure = structureBuilder.structure

    listener.traceTask(ScopeId.RootScopeId, BuildTargetScalacOptions) {
      for {
        target <- targets
        scopeId <- target.asScopeId
      } yield {
        if (scopeId == ScopeId.RootScopeId)
          new ScalacOptionsItem(target, Nil.asJava, Nil.asJava, intellijFriendlyURI(fakeOutputs))
        else {
          val resolvedScope = structure.scopes(scopeId)

          val internalSparseClasspath =
            resolvedScope.sparseInternalCompileDependencies.map { id =>
              val jar = sparseJarPathBuilder.libDir(id).resolveJar(NamingConventions.scopeOutputName(id))
              intellijFriendlyURI(jar, alwaysIncludeSources = true)
            }
          val externalClasspath = resolvedScope.externalCompileDependencies.map(intellijFriendlyURI)

          // we need to create empty directories for IntelliJ
          val buildDir = fakeOutputs.resolveDir(scopeId.toString)
          if (!buildDir.exists) Files.createDirectory(buildDir.path)

          new ScalacOptionsItem(
            target,
            resolvedScope.config.scalacConfig.options.asJava,
            (internalSparseClasspath ++ externalClasspath).asJava,
            intellijFriendlyURI(buildDir.path)
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
      pythonEnabled: Boolean,
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

    if (usingPython) {
      res.setDataKind(BuildTargetDataKind.PYTHON)
      res.setData(
        new PythonBuildTarget(
          pythonConfig.map(_.python.version).getOrElse("MISSING VERSION"),
          pythonConfig.map(_.python.path).getOrElse("MISSING_INTERPRETER_LOCATION")
        )
      )
    } else {
      scalaConfig match {
        case Some(cfg) =>
          res.setDataKind(BuildTargetDataKind.SCALA)
          val scalaTarget = new ScalaBuildTarget(
            "org.scala-lang",
            cfg.scalaVersion,
            cfg.scalaMajorVersion,
            ScalaPlatform.JVM,
            cfg.scalaJars.map(intellijFriendlyURI(_, alwaysIncludeSources = false)).asJava
          )
          javaConfig.foreach(c => scalaTarget.setJvmBuildTarget(asJvmBuildTarget(c)))
          res.setData(scalaTarget)
        case None =>
          javaConfig match {
            case Some(cfg) =>
              res.setDataKind(BuildTargetDataKind.JVM)
              res.setData(asJvmBuildTarget(cfg))
            case None =>
              configHash.foreach { hash =>
                res.setDataKind(BuildServerProtocolService.ConfigHash)
                res.setData(hash)
              }
          }
      }
    }
    res
  }

  private def asJvmBuildTarget(javaConfig: JavacConfiguration): JvmBuildTarget = {
    val javaRelease = javaConfig.release
    // IntelliJ refers to Java as 1.x for x < 10 (see com.intellij.pom.java.LanguageLevel)
    val javaReleaseString = if (javaRelease < 10) s"1.$javaRelease" else javaRelease.toString
    val jdkUri = intellijFriendlyURI(javaConfig.jdkHome.path)
    val jvmBuildTarget = new JvmBuildTarget(jdkUri, javaReleaseString)
    jvmBuildTarget
  }
  private def asBuildTarget(
      scopeInfo: ScopeInfo,
      scalaConfig: Option[ScalaVersionConfig],
      pythonEnabled: Boolean
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
      pythonEnabled
    )
  }

  @closuresEnterGraph
  private def run[A](task: => A): CompletableFuture[A] =
    workspace.run(cancellationScope, listener)(task).exceptionally { t =>
      val (unwrapped, message) = translateException("Workspace refresh failed")(t)
      val error = new ResponseError(
        ResponseErrorCode.InternalError,
        message.getOrElse(unwrapped.toString),
        unwrapped
      )
      throw new ResponseErrorException(error)
    }

  private def intellijFriendlyURI(path: Path): String = {
    val uri = PathUtils.uriString(path)
    // we can't use `path.startsWith(root)`, because `path.startsWith` requires at least two elements for a
    // network path and root may only be one element long, so instead we use `path.toString.startsWith(...)`
    if (path.toString.startsWith(s"${NamingConventions.AfsRootStr.replace('/', '\\')}"))
      uri.replaceFirst(s"${NamingConventions.AfsRootStr}", s"/${NamingConventions.AfsRootMapping}")
    else uri
  }

  private def intellijFriendlyURI(jar: JarAsset, alwaysIncludeSources: Boolean = false): String = {
    val coreUrl = intellijFriendlyURI(jar.path)
    val srcJar = {
      val afsSrc = jar.parent.resolveJar(jar.name.replace(".jar", ".src.jar"))
      if (afsSrc.exists) afsSrc else jar.parent.resolveJar(jar.name.replace(".jar", "-sources.jar"))
    }
    val javadocJar = {
      val afsDoc = jar.parent.resolveJar(jar.name.replace(".jar", ".javadoc.jar"))
      if (afsDoc.exists) afsDoc else jar.parent.resolveJar(jar.name.replace(".jar", "-javadoc.jar"))
    }
    val additions = mutable.Buffer[String]()
    if (alwaysIncludeSources || srcJar.exists) additions += s"src=${intellijFriendlyURI(srcJar.path)}"
    if (javadocJar.exists) additions += s"javadoc=${intellijFriendlyURI(javadocJar.path)}"
    if (additions.isEmpty) coreUrl else s"$coreUrl${additions.mkString("?", "&", "")}"
  }

  private def intellijFriendlyURI(d: Directory): String = intellijFriendlyURI(d.path)

  private def intellijFriendlyURI(e: ExternalClassFileArtifact): String = {
    val coreUrl = intellijFriendlyURI(e.path)
    val additions = Seq(e.source.map("src" -> _), e.javadoc.map("javadoc" -> _)).collect { case Some((tpe, a)) =>
      s"$tpe=${intellijFriendlyURI(a.path)}"
    }
    if (additions.isEmpty) coreUrl else s"$coreUrl${additions.mkString("?", "&", "")}"
  }

  private def vsCodeFriendlyPath(p: Pathed): String = {
    val pathStr = p.pathString
    if (isWindows && pathStr.startsWith(NamingConventions.AfsRootStr))
      pathStr.replaceFirst(NamingConventions.AfsRootStr, NamingConventions.AfsRootMapping)
    else pathStr
  }

  private def findMainScopeId(ids: Seq[ScopeId], p: ScopeId): Option[ScopeId] =
    ids.find(_.isMain).orElse {
      if (p.module.endsWith("test")) ids.find(_.tpe == "test")
      else None
    }

}
