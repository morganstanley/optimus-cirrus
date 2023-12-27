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

import java.io.IOException
import java.nio.file.Files

import optimus.buildtool.compilers.Task
import optimus.buildtool.files.JarAsset
import optimus.platform._
import xsbti.compile.CompileProgress

class ObtProgressReporter(
    activeTask: => Task,
    jars: Jars,
    previousSignatureAnalysisJar: Option[JarAsset],
    onSignaturesComplete: Option[() => Unit]
) extends CompileProgress {
  private var prevPhase: String = _
  override def startUnit(phase: String, unitPath: String): Unit = ()
  // (note that currentPhase is called prevPhase on the CompileProgress interface, but if you look where this method
  // is called from, the phase has already been advanced at that point)
  override def advance(current: Int, total: Int, currentPhase: String, nextPhase: String): Boolean = {
    // this method is called once per source file, but we only notify when we move to a new phase to avoid overloading
    // the status change callback with messages (especially important in BSP mode where that would flood the client).
    // Zinc's AnalyzingJavaCompiler reports a phase of "<some phase>"; we suppress this since it's not useful.
    if (prevPhase != currentPhase && currentPhase != "<some phase>") {
      prevPhase = currentPhase
      activeTask.trace.reportProgress(currentPhase.toLowerCase)
    }
    !EvaluationContext.cancelScope.isCancelled
  }

  override def afterEarlyOutput(success: Boolean): Unit =
    try {
      (jars.signatureAnalysisJar, previousSignatureAnalysisJar) match {
        case (Some(analysis), Some(prevAnalysis)) =>
          // if Zinc successfully completes with no changes then the signature analysis won't have been written, but the
          // previous is still valid so copy it forward
          if (success && !analysis.tempPath.existsUnsafe && prevAnalysis.existsUnsafe)
            Files.copy(prevAnalysis.path, analysis.tempPath.path)
        case _ => // do nothing
      }

      // note that we don't need to copy signatures forward because that's already done before we invoked Zinc
      onSignaturesComplete.foreach(_())
    } catch {
      case ioe: IOException =>
        // rethrow IOException as a RuntimeException since scala.tools.nsc.Global.Run.compileFiles catches
        // IOExceptions and turns them into standard compile errors
        // TODO (OPTIMUS-45208): Remove this once Global.Run.compileFiles is changed
        throw new RuntimeException(ioe)
    }
}
