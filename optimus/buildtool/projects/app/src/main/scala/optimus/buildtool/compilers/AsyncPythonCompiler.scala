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
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.artifacts.PythonArtifact
import optimus.buildtool.artifacts.PythonMetadata
import optimus.buildtool.compilers.AsyncPythonCompiler.Inputs
import optimus.buildtool.compilers.venv.AsyncVenvCommandRunner
import optimus.buildtool.compilers.venv.VenvUtils
import optimus.buildtool.config.PythonConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Python
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.SandboxFactory
import optimus.buildtool.utils.TarUtils
import optimus.buildtool.utils.Utils
import optimus.exceptions.RTException
import optimus.platform.NodeFunction0
import optimus.platform.NodeTry
import optimus.platform.entity
import optimus.platform.node
import org.apache.commons.compress.compressors.gzip.GzipParameters

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

object AsyncPythonCompiler {
  final case class Inputs(
      sourceFiles: SortedMap[SourceUnitId, HashedContent],
      outputJar: FileAsset,
      pythonConfig: PythonConfiguration,
      inputsHash: String
  )
}

@entity trait AsyncPythonCompiler {
  @node def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): Option[PythonArtifact]
}

@entity private[buildtool] class AsyncPythonCompilerImpl(
    pipCache: Directory,
    venvCache: Directory,
    sandboxFactory: SandboxFactory,
    logDir: Directory,
    credentialFile: String,
    pythonEnabled: NodeFunction0[Boolean])
    extends AsyncPythonCompiler {

  private def venvLogFile(logDir: Directory, id: ScopeId): FileAsset =
    logDir.resolveFile(s"${OptimusBuildToolBootstrap.generateLogFilePrefix()}.$id.venvCmd.log")

  @node override def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): Option[PythonArtifact] = {
    if (OsUtils.isWindows && !pythonEnabled()) None
    else Some(buildArtifact(scopeId, inputs))
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

      val venvName = s"venv-${scopeId.module}"
      val (compilationFailed, compilationMessages) = {
        val messages =
          AsyncVenvCommandRunner.createVenv(
            venvName,
            pythonConfig,
            sandbox.buildDir,
            pipCache,
            venvCache,
            credentialFile)
        (MessagesArtifact.hasErrors(messages), messages)
      }

      val gzipParams = new GzipParameters()
      /*
        level 1 was chosen because it benefits large artifacts and is around the same as default level for small ones.
        (the difference is about 10% in size, and its faster about 50%)
       */
      gzipParams.setCompressionLevel(1)
      TarUtils.populateTarGz(outputJar, sandbox.buildDir, gzipParams) { stream =>
        import TarUtils._
        import optimus.buildtool.artifacts.JsonImplicits._
        addFileToTarGz(stream, sandbox.sourceDir.path)

        val metadata = PythonMetadata(
          OsUtils.osVersion,
          compilationMessages,
          hasErrors = compilationFailed,
          resolvedInputs.inputsHash)
        AssetUtils.withTempJson(metadata) { json =>
          tarWriteFile(stream, CachedMetadata.MetadataFile.toString, json.toString)
        }

        if (pythonConfig.isAfs) {
          val pathToPth = s"${VenvUtils.sitePackages(venvName, pythonConfig)}/libs.pth"
          val content = pythonConfig.afsDependencies.map(_.path).mkString("\n")
          tarWriteFile(stream, pathToPth, content)
        }
      }

      val artifact = PythonArtifact.create(
        scopeId,
        outputJar,
        OsUtils.osVersion,
        compilationMessages,
        compilationFailed,
        resolvedInputs.inputsHash
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
