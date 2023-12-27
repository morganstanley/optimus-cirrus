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
package optimus.buildtool.compilers

import java.nio.file.Files
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CppMetadata
import optimus.buildtool.artifacts.InternalCppArtifact
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.compilers.AsyncCppCompiler.Inputs
import optimus.buildtool.compilers.cpp.CppCompilerFactory
import optimus.buildtool.compilers.cpp.CppFileCompiler
import optimus.buildtool.compilers.cpp.CppFileCompiler.Output
import optimus.buildtool.compilers.cpp.CppLinker
import optimus.buildtool.config.CppBuildConfiguration
import optimus.buildtool.config.CppConfiguration
import optimus.buildtool.config.CppToolchain
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.Cpp
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.Sandbox
import optimus.buildtool.utils.SandboxFactory
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.Utils.OsVersionMismatchException
import optimus.exceptions.RTException
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

@entity trait AsyncCppCompiler {
  @node def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): Option[InternalCppArtifact]
}

object AsyncCppCompiler {
  final case class Inputs(
      sourceFiles: SortedMap[SourceUnitId, HashedContent],
      outputJar: JarAsset,
      cppConfig: CppConfiguration,
      inputArtifacts: Seq[Artifact]
  )

  val UpstreamDir = "upstream"
  val HeaderDir = "header"

  sealed abstract class BuildType(val name: String) {
    def config(cppConfig: CppConfiguration): Option[CppBuildConfiguration]
  }
  object BuildType {
    case object Release extends BuildType("release") {
      override def config(cppConfig: CppConfiguration): Option[CppBuildConfiguration] = cppConfig.release
    }
    case object Debug extends BuildType("debug") {
      override def config(cppConfig: CppConfiguration): Option[CppBuildConfiguration] = cppConfig.debug
    }
  }
}

@entity private[buildtool] class AsyncCppCompilerImpl(
    factory: CppCompilerFactory,
    sandboxFactory: SandboxFactory,
    requiredOsVersions: Set[String]
) extends AsyncCppCompiler {
  import AsyncCppCompiler._

  @node def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): Option[InternalCppArtifact] = {
    val resolvedInputs = inputs()
    import resolvedInputs._

    val prefix = Utils.logPrefix(scopeId, Cpp, Some(cppConfig.osVersion))

    // If both are `NoToolchain` then we can legitimately skip building
    if (
      !cppConfig.release.map(_.toolchain).contains(CppToolchain.NoToolchain) ||
      !cppConfig.debug.map(_.toolchain).contains(CppToolchain.NoToolchain)
    ) {
      if (cppConfig.osVersion == OsUtils.osVersion) {
        val trace = ObtTrace.startTask(scopeId, Cpp)

        val sandbox = sandboxFactory(s"${scopeId.properPath}-cpp", sourceFiles)
        val result = NodeTry {

          log.info(s"${prefix}Starting compilation")
          log.debug(s"${prefix}Output paths: ${sandbox.root} -> ${outputJar.pathString}")

          val upstreamHeadersDir = sandbox.root.resolveDir(UpstreamDir).resolveDir(HeaderDir)
          val upstreamHeaders = inputArtifacts.apar.collect { case a: InternalCppArtifact =>
            Jars.withJar(a.file) { root =>
              // OK to use unsafe here since we're dealing with an RT jar
              val headers = Directory.listFilesUnsafe(root.resolveDir(HeaderDir))
              Utils.createDirectories(upstreamHeadersDir)
              headers.map { h =>
                val target = upstreamHeadersDir.resolveFile(h.name)
                Files.copy(h.path, target.path)
                target
              }
            }
          }.flatten

          val internalIncludes = if (upstreamHeaders.nonEmpty) Seq(upstreamHeadersDir) else Nil

          val cppSources = sandbox.sources.apar.filter { case (_, f) => !SourceFolder.isCppHeaderFile(f) }
          log.info(
            s"$prefix${cppSources.size}/${cppSources.size} cpp invalidations (non-incremental): ${cppSources.map(_._2.name).mkString(", ")}"
          )

          val headerDir = outputDir(sandbox, HeaderDir)

          val cppHeaders = sandbox.sources.apar.filter { case (_, f) => SourceFolder.isCppHeaderFile(f) }
          cppHeaders.foreach { case (_, f) =>
            Files.copy(f.path, headerDir.resolveFile(f.name).path)
          }

          val (releaseFile, releaseMessages) =
            compileAndLink(scopeId, inputs, cppSources, headerDir, internalIncludes, sandbox, BuildType.Release)
          val (debugFile, debugMessages) =
            compileAndLink(scopeId, inputs, cppSources, headerDir, internalIncludes, sandbox, BuildType.Debug)

          // `distinct` because we could have duplicates here due to compiling the same sources twice
          val messages = (releaseMessages ++ debugMessages).distinct

          val hasErrors = MessagesArtifact.hasErrors(messages)
          val releasePath = releaseFile.map(sandbox.buildDir.relativize)
          val debugPath = debugFile.map(sandbox.buildDir.relativize)

          val artifact = createArtifact(
            scopeId,
            outputJar,
            Some(sandbox.buildDir),
            OsUtils.osVersion,
            releasePath,
            debugPath,
            messages
          )

          log.info(s"${prefix}Completing compilation")
          log.debug(s"${prefix}Output: $artifact")

          trace.end(success = !hasErrors, artifact.errors, artifact.warnings)

          Some(artifact)
        } getOrRecover { case e @ RTException =>
          trace.publishMessages(Seq(CompilationMessage.error(e)))
          trace.end(success = false)
          throw e
        }
        sandbox.close()
        result
      } else if (requiredOsVersions.contains(cppConfig.osVersion)) {
        throw new OsVersionMismatchException(cppConfig.osVersion)
      } else {
        None
      }
    } else {
      log.info(s"${prefix}Skipping compilation")
      val messages = Seq(CompilationMessage.info(s"No cpp configuration provided for $scopeId"))
      Some(createArtifact(scopeId, outputJar, None, cppConfig.osVersion, None, None, messages))
    }

  }

  @node private def compileAndLink(
      scopeId: ScopeId,
      inputs: NodeFunction0[Inputs],
      cppSources: Seq[(SourceUnitId, FileAsset)],
      headerDir: Directory,
      internalIncludes: Seq[Directory],
      sandbox: Sandbox,
      buildType: BuildType
  ): (Option[FileAsset], Seq[CompilationMessage]) = {
    val resolvedInputs = inputs()
    import resolvedInputs._

    buildType.config(cppConfig) match {
      case Some(buildConfig) if buildConfig.toolchain != CppToolchain.NoToolchain =>
        val compiler = factory.newCompiler(scopeId, buildConfig.toolchain)

        val objectDir = outputDir(sandbox, buildType.name)

        val (precompileSources, otherSources) = cppSources.partition { case (id, _) =>
          buildConfig.precompiledHeader.contains(id.sourceFolderToFilePath)
        }

        def compile(id: SourceUnitId, file: FileAsset, createPrecompiledHeader: Boolean): Output = {
          val precompiledHeader = buildConfig.precompiledHeader.map { case (_, header) =>
            CppFileCompiler.PrecompiledHeader(header, headerDir, createPrecompiledHeader)
          }.toIndexedSeq
          val fileInputs =
            CppFileCompiler.FileInputs(
              file,
              id,
              buildConfig,
              sandbox.root,
              internalIncludes,
              objectDir,
              precompiledHeader
            )
          compiler.compile(fileInputs)
        }

        compiler.setup(buildConfig, sandbox.root)

        val (precompileOutputs, otherOutputs) = aseq(
          precompileSources.apar.map { case (id, f) => compile(id, f, createPrecompiledHeader = true) },
          otherSources.apar.map { case (id, f) => compile(id, f, createPrecompiledHeader = false) }
        )

        val compileOutputs = precompileOutputs ++ otherOutputs

        val compileMessages = compileOutputs.flatMap(_.messages)

        val linkOutput = if (buildConfig.outputType.isDefined && !MessagesArtifact.hasErrors(compileMessages)) {
          val upstreamObjectsDir = sandbox.root.resolveDir(UpstreamDir).resolveDir(buildType.name)
          val upstreamObjects = inputArtifacts.apar.collect { case a: InternalCppArtifact =>
            Jars.withJar(a.file) { root =>
              // OK to use unsafe here since we're dealing with an RT jar
              val upstreamFiles = Directory.listFilesUnsafe(root.resolveDir(buildType.name))
              Utils.createDirectories(upstreamObjectsDir)
              upstreamFiles.flatMap { o =>
                val target = upstreamObjectsDir.resolveFile(o.name)
                Files.copy(o.path, target.path)
                // copy debug files but don't include them in the list of upstream objects
                if (target.name.endsWith(".obj")) Some(target) else None
              }
            }
          }.flatten

          val objectFiles = compileOutputs.flatMap(_.objectFile) ++ upstreamObjects

          val linker = factory.newLinker(scopeId, buildConfig.toolchain)

          Some(
            linker.link(
              CppLinker.LinkInputs(objectFiles, buildConfig, sandbox.root, sandbox.buildDir, buildType)
            )
          )
        } else None

        (
          linkOutput.flatMap(_.outputFile),
          compileMessages ++ linkOutput.map(_.messages).getOrElse(Nil)
        )

      case Some(_) => // buildConfig.cppToolchain == NoToolchain
        (None, Seq(CompilationMessage.info(s"No cpp ${buildType.name} configuration provided for $scopeId")))
      case None =>
        (None, Seq(CompilationMessage.error(s"No cpp ${buildType.name} configuration provided for $scopeId")))
    }

  }

  private def outputDir(sandbox: Sandbox, name: String): Directory = {
    val d = sandbox.outputDir(name)
    Files.createDirectories(d.path)
    d
  }

  @node private def createArtifact(
      scopeId: ScopeId,
      outputJar: JarAsset,
      buildDir: Option[Directory],
      osVersion: String,
      releasePath: Option[RelativePath],
      debugPath: Option[RelativePath],
      messages: Seq[CompilationMessage]
  ): InternalCppArtifact = {
    val hasErrors = MessagesArtifact.hasErrors(messages)

    val metadata = CppMetadata(osVersion, releasePath, debugPath, messages, hasErrors)

    AssetUtils.atomicallyWrite(outputJar) { tmp =>
      import optimus.buildtool.artifacts.JsonImplicits._
      Jars.createJar(JarAsset(tmp), metadata, buildDir)()
    }

    InternalCppArtifact.create(
      scopeId = scopeId,
      file = outputJar,
      precomputedContentsHash = Hashing.hashFileContent(outputJar),
      osVersion = osVersion,
      release = releasePath,
      debug = debugPath,
      messages = messages,
      hasErrors = hasErrors
    )
  }

}

object AsyncCppCompilerImpl {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  // This is the node through which compilation is initiated. It's very important that we don't lose these from
  // cache (while they are still running at least) because that can result in recompilations of the same scope
  // due to a (potentially large) race between checking if the output artifacts are on disk and actually writing
  // them there after compilation completes.
  artifact.setCustomCache(reallyBigCache)

}
