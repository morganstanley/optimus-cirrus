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
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.artifacts.Severity
import optimus.buildtool.compilers.cpp.CppFileCompiler
import optimus.buildtool.compilers.cpp.CppUtils
import optimus.buildtool.config.CppBuildConfiguration
import optimus.buildtool.config.CppConfiguration.CompilerFlag
import optimus.buildtool.config.CppConfiguration.CompilerFlag._
import optimus.buildtool.config.CppToolchain
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.utils.TypeClasses._

import scala.collection.immutable.Seq

class GccCompiler(scopeId: ScopeId, toolchain: CppToolchain) extends CppFileCompiler {
  import CppFileCompiler._
  import GccCompiler._

  override def setup(cppConfiguration: CppBuildConfiguration, sandbox: Directory): Unit = {
    val sysRoot = sandbox.resolveDir("sysroot")

    toolchain.includes.foreach { d =>
      GccUtils.copyToSandbox(d, sysRoot)
    }
  }

  override def compile(inputs: FileInputs): Output = {
    val prefix = s"[$scopeId:cpp:${inputs.sourceFile.name}]"

    val gcc = toolchain.compiler

    val filePrefix = NamingConventions.prefix(inputs.sourceFile.name)
    val objectFile = inputs.objectDir.resolveFile(s"$filePrefix.o")
    val dependencyFile = inputs.objectDir.resolveFile(s"$filePrefix.d")

    val sysRoot = inputs.sandbox.resolveDir("sysroot")

    val translatedFlags = translate(
      toolchain.compilerFlags ++ inputs.cppConfiguration.compilerFlags,
      inputs.cppConfiguration.warningLevel orElse toolchain.warningLevel
    )

    val (flagMessages, flagArgs) = translatedFlags.separate

    val cmdLine = Seq(
      gcc.path.toString,
      "-c", // no linking
      "-fdiagnostics-color=never", // don't write colored output to the console
      "-fno-canonical-system-headers", // don't shorten system header paths
      "-fPIC", // generate position independent code for shared libraries
      s"-fdebug-prefix-map=${inputs.sandbox.path.toString}=/${scopeId.properPath}/", // strip out sandbox-dependent paths
      s"--sysroot=${sysRoot.path.toString}" // ensure we don't use uncaptured dependencies
    ) ++
      inputs.internalIncludes.map(i => s"-I${i.path.toString}") ++
      (inputs.cppConfiguration.includes ++ toolchain.includes).flatMap(i => Seq("-isystem", i.path.toString)) ++
      (toolchain.symbols ++ inputs.cppConfiguration.symbols).map(s => s"-D$s") ++
      flagArgs.filter(_.nonEmpty) ++
      Seq(
        "-o",
        objectFile.path.toString,
        "-MMD",
        "-MP",
        "-MF",
        dependencyFile.path.toString
      ) ++
      toolchain.compilerArgs ++
      inputs.cppConfiguration.compilerArgs :+
      s"${inputs.sourceFile.name}"

    val inputFile = inputs.sourceId.localRootToFilePath

    val processMessages = CppUtils.launch(
      processType = "Compiler",
      prefix = prefix,
      cmdLine = cmdLine,
      inputFile = Some(inputFile),
      workingDir = Some(inputs.sourceFile.parent),
      paths = Map(
        "PATH" -> toolchain.compilerPath,
        "INCLUDE" -> toolchain.includes
      )
    ) {
      case GccCompiler.Error(line, column, msg) =>
        CompilationMessage(Some(MessagePosition(inputFile.pathString, line.toInt, column.toInt)), msg, Severity.Error)
      case GccCompiler.UnpositionedError(msg) =>
        CompilationMessage(Some(MessagePosition(inputFile.pathString)), msg, Severity.Error)
      case GccCompiler.Warning(line, column, msg) =>
        CompilationMessage(Some(MessagePosition(inputFile.pathString, line.toInt, column.toInt)), msg, Severity.Warning)
    }

    val pchWarning = if (inputs.precompiledHeaders.nonEmpty) {
      Some(
        CompilationMessage(
          Some(MessagePosition(inputFile.pathString)),
          "Precompiled headers not currently supported for gcc",
          Severity.Warning
        )
      )
    } else None

    val messages = flagMessages ++ processMessages ++ pchWarning

    if (!messages.exists(_.isError)) Output(Some(objectFile), messages)
    else Output(None, messages)
  }

}

object GccCompiler {
  private val Error = """.+?([0-9]+):([0-9]+): (?:[\w]+ )?error: (.*)""".r
  private val UnpositionedError = """(?:[\w]+ )?error: (.*)""".r
  private val Warning = """.+?([0-9]+):([0-9]+): warning: (.*)""".r

  private[gcc] def translate(
      flags: Set[CompilerFlag],
      warningLevel: Option[Int]
  ): Seq[Either[CompilationMessage, String]] =
    flags.toIndexedSeq.map {
      case CompressDebugSections  => Right("-Wa,--compress-debug-sections")
      case Debug                  => Right("-g")
      case FatalWarnings          => Right("-Werror")
      case NoExecStack            => Right("-Wa,--noexecstack")
      case LanguageStandard.Cpp11 => Right("-std=c++11")
      case Optimizations.Fast     => Right("-O2")
      case x                      => Left(CompilationMessage.warning(s"Unsupported compiler flag: $x"))
    } ++ warningLevel.toIndexedSeq.flatMap { w =>
      if (w <= 0) Nil
      else if (w == 1 || w == 2) Seq(Right("-Wall"))
      else Seq(Right("-Wall"), Right("-Wextra"))
    }
}
