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
package optimus.buildtool.compilers.cpp.gcc

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.Severity
import optimus.buildtool.compilers.cpp.CppLinker
import optimus.buildtool.compilers.cpp.CppUtils
import optimus.buildtool.config.CppConfiguration.LinkerFlag
import optimus.buildtool.config.CppConfiguration.OutputType
import optimus.buildtool.config.CppToolchain
import optimus.buildtool.config.ScopeId
import optimus.buildtool.utils.TypeClasses._

import scala.collection.immutable.Seq

class GccLinker(scopeId: ScopeId, toolchain: CppToolchain) extends CppLinker {
  import CppLinker._
  import GccLinker._

  override def link(inputs: LinkInputs): Output = {

    val prefix = s"[$scopeId:cpp]"

    val link = toolchain.linker

    val outputFile =
      if (inputs.cppConfiguration.outputType.contains(OutputType.Library))
        inputs.outputDir.resolveFile(CppUtils.linuxNativeLibrary(scopeId, inputs.buildType))
      else
        inputs.outputDir.resolveFile(CppUtils.linuxNativeExecutable(scopeId, inputs.buildType))

    val sysRoot = inputs.sandbox.resolveDir("sysroot")

    toolchain.libPath.foreach { d =>
      GccUtils.copyToSandbox(d, sysRoot)
    }

    // Note here that we deliberately also pass compiler flags to the linker
    val translatedFlags =
      GccCompiler.translate(
        toolchain.compilerFlags ++ inputs.cppConfiguration.compilerFlags,
        inputs.cppConfiguration.warningLevel orElse toolchain.warningLevel
      ) ++
        translate(toolchain.linkerFlags ++ inputs.cppConfiguration.linkerFlags)

    val (flagMessages, flagArgs) = translatedFlags.separate

    val cmdLine = Seq(
      link.path.toString,
      "-fdiagnostics-color=never", // don't write colored output to the console
      "-fno-canonical-system-headers", // don't shorten system header paths
      "-fPIC", // generate position independent code for shared libraries
      "-Wl,--enable-new-dtags", // use RUNPATH rather than RPATH
      s"-fdebug-prefix-map=${inputs.sandbox.pathString}=/${scopeId.properPath}/", // strip out sandbox-dependent paths
      s"--sysroot=${sysRoot.path.toString}" // ensure we don't use uncaptured dependencies
    ) ++
      inputs.cppConfiguration.libs.map(l => s"-l$l") ++
      inputs.cppConfiguration.libPath.map(p => s"-L${p.path.toString}") ++
      inputs.cppConfiguration.systemLibs.map(_.path.toString) ++
      flagArgs ++
      Seq("-o", outputFile.path.toString) ++
      (if (inputs.cppConfiguration.outputType.contains(OutputType.Library)) Some("-shared") else None) ++
      toolchain.linkerArgs ++
      inputs.cppConfiguration.linkerArgs ++
      inputs.objectFiles.map(_.path.toString)

    val processMessages = CppUtils.launch(
      processType = "Linker",
      prefix = prefix,
      cmdLine = cmdLine,
      inputFile = None,
      workingDir = None,
      paths = Map("PATH" -> toolchain.linkerPath)
    ) {
      case GccLinker.Error(msgPrefix, msg) =>
        CompilationMessage(None, s"$msgPrefix: $msg", Severity.Error)
      case GccLinker.Warning(msgPrefix, msg) =>
        CompilationMessage(None, s"$msgPrefix: $msg", Severity.Warning)
    }

    val messages = flagMessages ++ processMessages

    if (!messages.exists(_.isError)) Output(Some(outputFile), messages)
    else Output(None, messages)
  }

}

object GccLinker {
  private val Error = """(.+?) : (?:[\w]+ )?error (.*)""".r
  private val Warning = """(.+?) : warning (.*)""".r

  private def translate(flags: Set[LinkerFlag]): Seq[Either[CompilationMessage, String]] =
    flags.toIndexedSeq.map { x =>
      Left(CompilationMessage(None, s"Unsupported linker flag: $x", Severity.Warning))
    }
}
