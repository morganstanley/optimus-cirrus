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
package optimus.buildtool.compilers.cpp.vc

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

class VcCompiler(scopeId: ScopeId, toolchain: CppToolchain) extends CppFileCompiler {
  import CppFileCompiler._

  override def setup(cppConfiguration: CppBuildConfiguration, sandbox: Directory): Unit = ()

  override def compile(inputs: FileInputs): Output = {
    val prefix = s"[$scopeId:cpp:${inputs.sourceFile.name}]"

    val cl = toolchain.compiler

    val filePrefix = NamingConventions.prefix(inputs.sourceFile.name)
    val objectFile = inputs.objectDir.resolveFile(s"$filePrefix.obj")
    val debugFile = inputs.objectDir.resolveFile(s"${scopeId.properPath}.pdb")

    val includes = inputs.internalIncludes ++ inputs.cppConfiguration.includes

    val translatedFlags = translate(
      toolchain.compilerFlags ++ inputs.cppConfiguration.compilerFlags,
      inputs.cppConfiguration.warningLevel orElse toolchain.warningLevel
    )

    val (flagMessages, flagArgs) = translatedFlags.separate

    val cmdLine = Seq(
      cl.path.toString,
      "/c", // no linking
      "/nologo", // no logo
      "/TP" // all sources are C++
    ) ++
      includes.map(i => s"/I${i.path.toString}") ++
      (toolchain.symbols ++ inputs.cppConfiguration.symbols).flatMap(s => Seq("/D", s)) ++
      flagArgs.filter(_.nonEmpty) ++
      inputs.precompiledHeaders.flatMap { case PrecompiledHeader(h, headerDir, create) =>
        val headerPrefix = NamingConventions.prefix(h.name)
        val output = headerDir.resolveFile(s"$headerPrefix.pch")
        val arg = if (create) "/Yc" else "/Yu"
        Seq(s"$arg${h.path.toString}", s"/Fp${output.path.toString}")
      } ++
      Seq(
        s"/Fo${objectFile.path.toString}",
        s"/Fd${debugFile.path.toString}"
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
      case VcCompiler.Error(line, msg) =>
        CompilationMessage(Some(MessagePosition(inputFile.pathString, line.toInt)), msg, Severity.Error)
      case VcCompiler.Warning(line, msg) =>
        CompilationMessage(Some(MessagePosition(inputFile.pathString, line.toInt)), msg, Severity.Warning)
    }

    val messages = flagMessages ++ processMessages

    if (!messages.exists(_.isError)) Output(Some(objectFile), messages)
    else Output(None, messages)
  }

  private def translate(flags: Set[CompilerFlag], warningLevel: Option[Int]): Seq[Either[CompilationMessage, String]] =
    flags.toIndexedSeq.map {
      case BufferWarnings                  => Right("/GS")
      case Debug                           => Right("/Zi")
      case FatalWarnings                   => Right("/WX")
      case FunctionLinking                 => Right("/Gy")
      case SecurityWarnings                => Right("/sdl")
      case CallingConvention.CDecl         => Right("/Gd")
      case CppExtensions.Inline            => Right("/Zc:inline")
      case CppExtensions.WcharBuiltin      => Right("/Zc:wchar_t")
      case Exceptions.NoExtern             => Right("/EHc")
      case Exceptions.StandardCpp          => Right("/EHs")
      case FloatingPoint.Precise           => Right("/fp:precise")
      case LanguageStandard.Cpp11          => Right("") // C++ 11 is the default for VC
      case Linking.Multithreaded           => Right("/MD")
      case Linking.MultithreadedExecutable => Right("/MT")
      case Optimizations.Fast              => Right("/O2")
      case Optimizations.Intrinsics        => Right("/Oi")
      case Optimizations.WholeProgram      => Right("/GL")
      case x                               => Left(CompilationMessage.warning(s"Unsupported compiler flag: $x"))
    } ++ warningLevel.map(w => Right(s"/W$w"))

}

object VcCompiler {
  private val Error = """.+?\(([0-9]+)\): (?:[\w]+ )?error (.*)""".r
  private val Warning = """.+?\(([0-9]+)\): warning (.*)""".r

  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}
