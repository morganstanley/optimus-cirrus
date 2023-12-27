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
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilationMessage._
import optimus.buildtool.artifacts.ElectronArtifact
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.artifacts.MessagesMetadata
import optimus.buildtool.builders.BackgroundProcessBuilder
import optimus.buildtool.compilers.AsyncElectronCompiler.Inputs
import optimus.buildtool.compilers.npm.AsyncNpmCommandRunner
import optimus.buildtool.config.ElectronConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.Electron
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing.hashFileOrDirectoryContent
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.SandboxFactory
import optimus.buildtool.utils.Utils
import optimus.exceptions.RTException
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

@entity trait AsyncElectronCompiler {
  @node def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): ElectronArtifact
}

object AsyncElectronCompiler {
  final case class Inputs(
      sourceFiles: SortedMap[SourceUnitId, HashedContent],
      outputJar: JarAsset,
      electronConfig: ElectronConfiguration,
      nodeVersion: String,
      pnpmVersion: String,
      inputArtifacts: Seq[Artifact])
}

object AsyncElectronCompilerImpl {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  // This is the node through which compilation is initiated. It's very important that we don't lose these from
  // cache (while they are still running at least) because that can result in recompilations of the same scope
  // due to a (potentially large) race between checking if the output artifacts are on disk and actually writing
  // them there after compilation completes.
  artifact.setCustomCache(reallyBigCache)

}

@entity private[buildtool] class AsyncElectronCompilerImpl(
    pnpmStoreDir: Directory,
    sandboxFactory: SandboxFactory,
    logDir: Directory)
    extends AsyncElectronCompiler {

  // one log file per scopeID, only be used for load debugging msg
  private def electronCmdLogFile(logDir: Directory, id: ScopeId): FileAsset =
    logDir.resolveFile(s"${OptimusBuildToolBootstrap.generateLogFilePrefix()}.$id.electronCmd.log")

  @node def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): ElectronArtifact = {
    val resolvedInputs = inputs()
    import resolvedInputs._

    val prefix = Utils.logPrefix(scopeId, Electron)
    val trace = ObtTrace.startTask(scopeId, Electron)
    val logFile = electronCmdLogFile(logDir, scopeId)

    NodeTry {
      log.info(s"${prefix}Starting compilation")

      val sandbox = sandboxFactory(s"${scopeId.properPath}-electron", sourceFiles)
      log.debug(s"${prefix}Output paths: ${sandbox.root} -> ${outputJar.pathString}")

      val (hasErrors: Boolean, compiledMessages: Seq[CompilationMessage]) = {
        val os = OsUtils.osType
        val commandTemplate = electronConfig.npmCommandTemplate.get(os).getOrThrow {
          s"Command template for OS '$os' is missing for scope $scopeId!"
        }
        val npmBuildCommands = electronConfig.npmBuildCommands.getOrElse(Nil)
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

        // This is needed for special case when npm generates links between folders
        AssetUtils.recursivelyDelete(sandbox.sourceDir.resolveDir("node_modules"), throwOnFail = true)

        val hasErrors = MessagesArtifact.hasErrors(compiledMessages)

        AssetUtils.atomicallyWrite(outputJar) { tmp =>
          import optimus.buildtool.artifacts.JsonImplicits._
          val tmpJar = JarAsset(tmp)
          val metadata = MessagesMetadata(compiledMessages, hasErrors = false)
          Jars.createJar(tmpJar, metadata, Some(sandbox.root.resolveDir("install")))(_ => {})
        }

        (hasErrors, compiledMessages)
      }

      val artifact = ElectronArtifact.create(scopeId, outputJar, hashFileOrDirectoryContent(outputJar))
      sandbox.close()
      trace.end(success = !hasErrors, compiledMessages.count(_.isError), compiledMessages.count(_.isWarning))
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
