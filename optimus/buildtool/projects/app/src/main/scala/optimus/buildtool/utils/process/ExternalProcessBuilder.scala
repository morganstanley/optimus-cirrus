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
package optimus.buildtool.utils.process

import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.trace.AsyncCategoryTrace
import optimus.buildtool.utils.Utils
import optimus.exceptions.RTExceptionTrait
import optimus.platform._
import optimus.platform.util.ProcessEnvironment

import java.io.File
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.collection.compat._
import scala.util.control.NonFatal

abstract class ExternalProcessException(
    val scopeId: ScopeId,
    val appName: String,
    message: String,
    cause: Throwable
) extends RuntimeException(message, cause)
    with RTExceptionTrait {
  def this(scopeId: ScopeId, appName: String, message: String) =
    this(scopeId, appName, message, null)
}

final class ProcessExitException(
    scopeId: ScopeId,
    appName: String,
    val exitCode: Int,
    message: String
) extends ExternalProcessException(scopeId, appName, message)
    with RTExceptionTrait {}

final class ProcessFailureException(
    scopeId: ScopeId,
    appName: String,
    message: String,
    cause: Throwable
) extends ExternalProcessException(scopeId, appName, message, cause)
    with RTExceptionTrait {
  def this(scopeId: ScopeId, appName: String, message: String) =
    this(scopeId, appName, message, null)
}

trait ExternalProcessBuilder {
  def build(
      scopeId: ScopeId,
      appName: String,
      cmdLine: Seq[String],
      category: Option[AsyncCategoryTrace] = None,
      envVariablesToAdd: Map[String, String] = Map.empty,
      envVariablesToClean: Seq[String] = Nil,
      envVariablesToRetain: Option[Set[String]] = None,
      workingDir: Option[Directory] = None,
      separateLogs: Boolean = false,
      lastLogLines: Int = 10
  ): ExternalProcess
}

object ExternalProcessBuilder {
  def javaCommand(
      classpathArtifacts: Seq[PathingArtifact],
      javaAgentArtifacts: Seq[PathingArtifact],
      javaOpts: Seq[String],
      mainClass: String,
      mainClassArgs: Seq[String]
  ): Seq[String] = {
    val classpathOpt = Seq("-cp", classpathArtifacts.map(_.pathString).mkString(File.pathSeparator))
    val agentOpts = javaAgentArtifacts.map(a => "-javaagent:" + a.pathString)
    Seq(Utils.javaExecutable) ++ ProcessEnvironment.moduleArguments ++ javaOpts ++ agentOpts ++ classpathOpt ++
      Seq(mainClass) ++ mainClassArgs
  }
}

trait ExternalProcess {
  @async def start(): Unit = startWithRetry(0, 0, 0)
  @async def startWithRetry(maxRetries: Int, msDelay: Long, showWarningsAfter: Int = 0): Unit
  def logFile: Option[FileAsset]
  def errFile: Option[FileAsset]
}

object ExternalProcess {
  private val log = getLogger(this)

  // `lastLogLines` is only used for logging, so safe to use `existsUnsafe` here
  def lastLogLines(outputFile: FileAsset, n: Int): Seq[String] = if (outputFile.existsUnsafe) {
    import org.apache.commons.io.input.ReversedLinesFileReader
    val rlfReader = ReversedLinesFileReader
      .builder()
      .setFile(outputFile.path.toFile)
      .setCharset(StandardCharsets.UTF_8)
      .get()
    val seqBuilder = IndexedSeq.newBuilder[String]

    @tailrec
    def readLogFile(totalLines: Int): Seq[String] = {
      val loadedLine = rlfReader.readLine()
      if (totalLines < 1 || loadedLine == null) seqBuilder.result().to(Seq).reverse
      else {
        seqBuilder += loadedLine
        readLogFile(totalLines - 1)
      }
    }

    try {
      readLogFile(n)
    } catch {
      case NonFatal(e) =>
        log.warn(s"Unexpected error when load log file: ${outputFile.pathString}", e)
        Nil
    } finally {
      rlfReader.close()
    }
  } else Nil
}
