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
import optimus.buildtool.compilers.cpp.CppLinker
import optimus.buildtool.compilers.cpp.CppUtils
import optimus.buildtool.config.CppConfiguration.LinkerFlag
import optimus.buildtool.config.CppConfiguration.LinkerFlag._
import optimus.buildtool.config.CppConfiguration.OutputType
import optimus.buildtool.config.CppToolchain
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.utils.TypeClasses._

import java.nio.file.Paths
import scala.collection.immutable.Seq

class VcLinker(scopeId: ScopeId, toolchain: CppToolchain) extends CppLinker {
  import CppLinker._

  override def link(inputs: LinkInputs): Output = {

    val prefix = s"[$scopeId:cpp]"

    val link = toolchain.linker

    val outputFile =
      if (inputs.cppConfiguration.outputType.contains(OutputType.Library))
        inputs.outputDir.resolveFile(CppUtils.windowsNativeLibrary(scopeId, inputs.buildType))
      else
        inputs.outputDir.resolveFile(CppUtils.windowsNativeExecutable(scopeId, inputs.buildType))
    val midFix = CppUtils.windowsMidfix(inputs.buildType)
    val libFile = inputs.outputDir.resolveFile(NamingConventions.scopeOutputName(scopeId, midFix, "lib"))
    val debugFile = inputs.outputDir.resolveFile(NamingConventions.scopeOutputName(scopeId, midFix, "pdb"))

    val translatedFlags = translate(
      toolchain.linkerFlags ++ inputs.cppConfiguration.linkerFlags,
      inputs.cppConfiguration.warningLevel orElse toolchain.warningLevel
    )

    val (flagMessages, flagArgs) = translatedFlags.separate

    val libPath = (inputs.cppConfiguration.libPath ++ inputs.cppConfiguration.systemLibs.map(_.parent)).distinct
    val systemLibNames = inputs.cppConfiguration.systemLibs.map(_.name)

    val cmdLine = Seq(
      link.path.toString,
      "/NOLOGO"
    ) ++
      inputs.cppConfiguration.libs.map(l => Paths.get(l).toString) ++
      libPath.map(p => s"/LIBPATH:${p.path.toString}") ++
      systemLibNames ++
      flagArgs ++
      Seq(
        "/MANIFEST",
        "/MANIFESTUAC:level='asInvoker' uiAccess='false'",
        "/manifest:embed"
      ) ++
      inputs.cppConfiguration.manifest.map(f => s"/manifestinput:${f.path.toString}") ++
      Seq(
        s"/OUT:${outputFile.path.toString}",
        s"/PDB:${debugFile.path.toString}",
        s"/IMPLIB:${libFile.path.toString}"
      ) ++
      (if (inputs.cppConfiguration.outputType.contains(OutputType.Library)) Some("/DLL") else None) ++
      toolchain.linkerArgs ++
      inputs.cppConfiguration.linkerArgs ++
      inputs.objectFiles.map(_.path.toString)

    val processMessages = CppUtils.launch(
      processType = "Linker",
      prefix = prefix,
      cmdLine = cmdLine,
      inputFile = None,
      workingDir = None,
      paths = Map(
        "PATH" -> toolchain.linkerPath,
        "LIB" -> toolchain.libPath
      )
    ) {
      case VcLinker.Error(msgPrefix, msg) =>
        CompilationMessage.error(s"$msgPrefix: $msg")
      case VcLinker.Warning(msgPrefix, msg) =>
        CompilationMessage.warning(s"$msgPrefix: $msg")
    }

    val messages = flagMessages ++ processMessages

    if (!messages.exists(_.isError)) Output(Some(outputFile), messages)
    else Output(None, messages)
  }

  private def translate(flags: Set[LinkerFlag], warningLevel: Option[Int]): Seq[Either[CompilationMessage, String]] =
    flags.toIndexedSeq.map {
      case Debug                                  => Right("/DEBUG")
      case NonIncremental                         => Right("/INCREMENTAL:NO")
      case Manifest.Embedded                      => Right("/MANIFEST:EMBED")
      case Optimizations.FoldDuplicates           => Right("/OPT:ICF")
      case Optimizations.RemoveUnreferenced       => Right("/OPT:REF")
      case Optimizations.WholeProgram.Incremental => Right("/LTCG:incremental")
      case Subsystem.Windows                      => Right("/SUBSYSTEM:WINDOWS")
      case x                                      => Left(CompilationMessage.warning(s"Unsupported linker flag: $x"))
    } ++ warningLevel.map(w => Right(s"/W$w"))
}

object VcLinker {
  private val Error = """(.+?) : (?:[\w]+ )?error (.*)""".r
  private val Warning = """(.+?) : warning (.*)""".r

  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}
