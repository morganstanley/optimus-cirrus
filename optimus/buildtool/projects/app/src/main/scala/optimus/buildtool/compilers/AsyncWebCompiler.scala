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

import optimus.buildtool.app.OptimusBuildToolBootstrap
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilationMessage._
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.artifacts.MessagesMetadata
import optimus.buildtool.builders.BackgroundProcessBuilder
import optimus.buildtool.compilers.AsyncWebCompiler.Inputs
import optimus.buildtool.compilers.npm.AsyncNpmCommandRunner
import optimus.buildtool.config.NpmConfiguration.NpmBuildMode._
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.WebConfiguration
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Web
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing.hashFileOrDirectoryContent
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.SandboxFactory
import optimus.buildtool.utils.Utils
import optimus.exceptions.RTException
import optimus.platform._

import java.io.PrintWriter
import java.nio.file.Files
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

@entity trait AsyncWebCompiler {
  @node def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): InternalClassFileArtifact
}

object AsyncWebCompiler {
  final case class Inputs(
      sourceFiles: SortedMap[SourceUnitId, HashedContent],
      outputJar: JarAsset,
      webConfig: WebConfiguration,
      nodeVersion: String,
      pnpmVersion: String,
      inputArtifacts: Seq[Artifact]
  )
}

object AsyncWebCompilerImpl {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  // This is the node through which compilation is initiated. It's very important that we don't lose these from
  // cache (while they are still running at least) because that can result in recompilations of the same scope
  // due to a (potentially large) race between checking if the output artifacts are on disk and actually writing
  // them there after compilation completes.
  artifact.setCustomCache(reallyBigCache)
}

@entity private[buildtool] class AsyncWebCompilerImpl(
    pnpmStoreDir: Directory,
    sandboxFactory: SandboxFactory,
    logDir: Directory)
    extends AsyncWebCompiler {
  // one log file per scopeID, only be used for load debugging msg
  private def webCmdLogFile(logDir: Directory, id: ScopeId): FileAsset =
    logDir.resolveFile(s"${OptimusBuildToolBootstrap.generateLogFilePrefix()}.$id.webCmd.log")

  @node override def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): InternalClassFileArtifact = {
    val resolvedInputs = inputs()
    import resolvedInputs._

    val prefix = Utils.logPrefix(scopeId, Web)
    val trace = ObtTrace.startTask(scopeId, Web)
    val logFile = webCmdLogFile(logDir, scopeId)

    NodeTry {
      log.info(s"${prefix}Starting compilation for mode ${webConfig.mode}")

      val sandboxFiles =
        if (webConfig.mode == Development) SortedMap.empty[SourceUnitId, HashedContent] else sourceFiles
      val sandbox = sandboxFactory(s"${scopeId.properPath}-web-${webConfig.mode}", sandboxFiles)
      // make sure build dir is created since it's needed in some build jobs.
      Utils.createDirectories(sandbox.buildDir)
      log.debug(s"${prefix}Output paths: ${sandbox.root} -> ${outputJar.pathString}")

      val (hasErrors: Boolean, compiledMessages: Seq[CompilationMessage]) = webConfig.mode match {
        case Production =>
          val os = OsUtils.osType
          val commandTemplate = webConfig.npmCommandTemplate.get(os).getOrThrow {
            s"Command template for OS '$os' is missing for scope $scopeId!"
          }
          val npmBuildCommands = webConfig.npmBuildCommands.getOrElse(Nil)
          val compiledMessages =
            if (npmBuildCommands.isEmpty) Seq(warning("No npm commands found."))
            else {
              val executedCmdMsgs = AsyncNpmCommandRunner.runNpmCmd(
                scopeId,
                commandTemplate,
                nodeVersion,
                pnpmVersion,
                npmBuildCommands,
                sandbox.sourceDir,
                pnpmStoreDir,
                logFile
              )
              BackgroundProcessBuilder
                .lastLogLines(logFile, 500)
                .foreach(log.debug(_)) // print max 500 lines for debugging
              writeNpmMetadata(sandbox.sourceDir, nodeVersion, pnpmVersion)
              executedCmdMsgs
            }

          // This is needed for special case when npm generates links between folders, also we don't want to include node_modules in the final bundle
          val nodeModulesDir = sandbox.sourceDir.resolveDir("node_modules")
          // It's safe to call existsUnsafe here - node_modules folder might/might not exist depends on whether the
          // lock file is in sync with package.json, but it's RT relative to the input (package.json + lock file)
          if (nodeModulesDir.existsUnsafe) AssetUtils.recursivelyDelete(nodeModulesDir, throwOnFail = true)

          AssetUtils.atomicallyWrite(outputJar) { tmp =>
            import optimus.buildtool.artifacts.JsonImplicits._
            val tmpJar = JarAsset(tmp)
            val metadata = MessagesMetadata(compiledMessages, hasErrors = false)
            Jars.createJar(tmpJar, metadata, Some(sandbox.buildDir)) { tempJarStream =>
              // Copy in source files (under "src/") so they're available for tests
              Directory.findFilesUnsafe(sandbox.sourceDir).foreach { f =>
                tempJarStream.copyInFile(f.path, sandbox.root.relativize(f))
              }
            }
          }

          // hasErrors should be always false cause compiledMessages won't contain any errors, when fail it will directly throw.
          (false, compiledMessages)

        case TestingResource =>
          val hasErrors = webConfig.npmBuildCommands.isDefined
          val compiledMessages =
            if (hasErrors) Seq(error(s"${prefix}Test resources will be packaged directly and won't run npm commands"))
            else {
              writeNpmMetadata(sandbox.sourceDir, nodeVersion, pnpmVersion)
              Seq(info(s"${prefix}Test resources packaged successfully"))
            }
          AssetUtils.atomicallyWrite(outputJar) { tmp =>
            Jars.createJar(JarAsset(tmp), compiledMessages, hasErrors = false, Some(sandbox.sourceDir))
          }
          (hasErrors, compiledMessages)

        case Development =>
          val path = sandbox.sourceDir.path.resolve(scopeId.toString)
          Files.createDirectories(path)
          val ignoreBuildFile = Files.createFile(path.resolve(".ignoredInObtBuild"))
          val message = s"${prefix}Skip compilation in dev mode"
          IO.using(new PrintWriter(Files.newOutputStream(ignoreBuildFile)))(_.println(message))
          val compiledMessages = Seq(warning(message))
          AssetUtils.atomicallyWrite(outputJar) { tmp =>
            Jars.createJar(JarAsset(tmp), compiledMessages, hasErrors = false, Some(sandbox.sourceDir))
          }
          (false, compiledMessages)

        case mode =>
          (true, Seq(error(s"${prefix}Unknown mode $mode")))
      }

      val artifact = InternalClassFileArtifact.create(
        InternalArtifactId(scopeId, ArtifactType.Resources, None),
        outputJar,
        hashFileOrDirectoryContent(outputJar),
        incremental = false
      )

      sandbox.close()
      val warnings = compiledMessages.filter(_.isWarning)
      trace.end(success = !hasErrors, compiledMessages.count(_.isError), warnings.size)
      warnings.foreach(w => log.debug(w.toString))
      log.info(s"${prefix}Completing compilation")
      artifact
    } getOrRecover { case e @ RTException =>
      trace.publishMessages(Seq(CompilationMessage.error(e)))
      trace.end(success = false)
      throw e
    }
  }

  private def writeNpmMetadata(target: Directory, nodeVersion: String, pnpmVersion: String): Unit = {
    Utils.writeStringsToFile(target.resolveFile(".nodeVersion").path, Seq(nodeVersion))
    Utils.writeStringsToFile(target.resolveFile(".pnpmVersion").path, Seq(pnpmVersion))
  }
}
