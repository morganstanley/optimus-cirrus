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
import optimus.buildtool.artifacts.CachedMetadata
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.JsonImplicits._
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.artifacts.PythonArtifact
import optimus.buildtool.artifacts.PythonMetadata
import optimus.buildtool.compilers.AsyncPythonCompiler.Inputs
import optimus.buildtool.compilers.venv.ThinPyappWrapper
import optimus.buildtool.config.PythonConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Python
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.SandboxFactory
import optimus.buildtool.utils.Utils
import optimus.exceptions.RTException
import optimus.platform.NodeFunction0
import optimus.platform.NodeTry
import optimus.platform.entity
import optimus.platform.node

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

object AsyncPythonCompiler {
  final case class Inputs(
      sourceFiles: SortedMap[SourceUnitId, HashedContent],
      outputTpa: FileAsset,
      pythonConfig: PythonConfiguration,
      inputsHash: String
  )
}

@entity trait AsyncPythonCompiler {
  @node def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): Option[PythonArtifact]
}

@entity private[buildtool] class AsyncPythonCompilerImpl(
    venvCache: Directory,
    uvCache: Directory,
    sandboxFactory: SandboxFactory,
    logDir: Directory,
    pipCredentialFile: String,
    uvCredentialFile: String,
    pythonEnabled: NodeFunction0[Boolean])
    extends AsyncPythonCompiler {

  private def venvLogFile(logDir: Directory, id: ScopeId): FileAsset =
    logDir.resolveFile(s"${OptimusBuildToolBootstrap.generateLogFilePrefix()}.$id.venvCmd.log")

  @node override def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): Option[PythonArtifact] = {
    if (pythonEnabled()) Some(buildArtifact(scopeId, inputs))
    else None
  }

  @node private def buildArtifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): PythonArtifact = {
    val resolvedInputs = inputs()
    import resolvedInputs._

    val prefix = Utils.logPrefix(scopeId, Python)
    val trace = ObtTrace.startTask(scopeId, Python)
    val logFile = venvLogFile(logDir, scopeId)

    NodeTry {
      log.info(s"${prefix}Starting compilation")

      val sandbox = sandboxFactory(s"${scopeId.properPath}-python-", sourceFiles)
      Utils.createDirectories(sandbox.buildDir)

      val (compilationFailed, compilationMessages) = {
        val messages =
          ThinPyappWrapper.createTpa(
            outputTpa.path,
            pythonConfig,
            sandbox,
            venvCache,
            uvCache,
            pipCredentialFile,
            uvCredentialFile)
        (MessagesArtifact.hasErrors(messages), messages)
      }

      Jars.withJar(outputTpa.asJar) { fs =>
        val metadata =
          PythonMetadata(
            OsUtils.osVersion,
            compilationMessages,
            hasErrors = compilationFailed,
            resolvedInputs.inputsHash,
            python = pythonConfig.python)

        AssetUtils.withTempJson(metadata) { json =>
          Files.write(fs.resolveFile(CachedMetadata.MetadataFile).path, json.toString.getBytes(StandardCharsets.UTF_8))
        }
      }

      val artifact = PythonArtifact.create(
        scopeId,
        outputTpa,
        OsUtils.osVersion,
        compilationMessages,
        compilationFailed,
        resolvedInputs.inputsHash,
        pythonConfig.python
      )

      sandbox.close()
      trace.end(success = !compilationFailed, compilationMessages.count(_.isError))
      log.info(s"${prefix}Completing compilation")
      artifact
    } getOrRecover { case e @ RTException =>
      trace.publishMessages(Seq(CompilationMessage.error(e)))
      trace.end(success = false)
      throw e
    }

  }
}

object AsyncPythonCompilerImpl {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  // This is the node through which compilation is initiated. It's very important that we don't lose these from
  // cache (while they are still running at least) because that can result in recompilations of the same scope
  // due to a (potentially large) race between checking if the output artifacts are on disk and actually writing
  // them there after compilation completes.
  artifact.setCustomCache(reallyBigCache)
}
