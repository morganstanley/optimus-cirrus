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
package optimus.buildtool.config

import CppConfiguration.OutputType
import optimus.buildtool.config.CppConfiguration.CompilerFlag
import optimus.buildtool.config.CppConfiguration.LinkerFlag
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.utils.PathUtils.{fingerprintAsset => fa}

import scala.collection.compat._
import scala.collection.immutable.Seq

final case class CppConfiguration(
    osVersion: String,
    release: Option[CppBuildConfiguration],
    debug: Option[CppBuildConfiguration]
) {
  def fingerprint: Seq[String] =
    if (!isEmpty) Seq(s"[OS]$osVersion") ++ fp(release).map(s => s"[Release]$s") ++ fp(debug).map(s => s"[Debug]$s")
    else Nil

  def isEmpty: Boolean = release.isEmpty && debug.isEmpty

  private def fp(cfg: Option[CppBuildConfiguration]): Seq[String] = cfg.map(_.fingerprint).getOrElse(Nil)
}

final case class CppBuildConfiguration(
    toolchain: CppToolchain,
    outputType: Option[OutputType],
    preload: Boolean,
    compilerFlags: Set[CompilerFlag],
    symbols: Seq[String],
    includes: Seq[Directory],
    warningLevel: Option[Int], // 0 to 4
    // source (eg. foo.cpp) -> header for precompilation (eg. foo.h)
    precompiledHeader: Map[RelativePath, RelativePath],
    compilerArgs: Seq[String],
    linkerFlags: Set[LinkerFlag],
    libs: Seq[String], // for vc this is a file path but for gcc it's just the "-l" arguments
    libPath: Seq[Directory],
    systemLibs: Seq[FileAsset],
    manifest: Option[FileAsset],
    linkerArgs: Seq[String],
    fallbackPath: Seq[Directory]
) {

  def fingerprint: Seq[String] = toolchain match {
    case CppToolchain.NoToolchain => Seq("[Toolchain]None")
    case tc =>
      tc.fingerprint ++
        outputType.map(t => s"[OutputType]${t.getClass.getSimpleName}") ++
        compilerFlags.map(f => s"[CompilerFlag]${f.getClass.getName}").to(Seq).sorted ++
        symbols.map(s => s"[Symbol]$s") ++
        includes.map(fa("Include", _)) ++
        warningLevel.map(l => s"[WarningLevel]$l") ++
        precompiledHeader.map { case (s, h) => s"[PrecompiledHeader]$s:$h" } ++
        compilerArgs.map(a => s"[CompilerArg]$a") ++
        linkerFlags.map(f => s"[LinkerFlag]${f.getClass.getName}").to(Seq).sorted ++
        libs.map(l => s"[Lib]$l") ++
        libPath.map(fa("LibPath", _)) ++
        systemLibs.map(fa("SystemLib", _)) ++
        manifest.map(fa("Manifest", _)) ++
        linkerArgs.map(a => s"[LinkerArg]$a")
    // Note: fallbackPath doesn't affect the C++ artifacts created, so doesn't need to be part of the fingerprint
  }
}

object CppConfiguration {
  sealed trait OutputType
  object OutputType {
    case object Library extends OutputType
    case object Executable extends OutputType
  }

  sealed trait Flag

  sealed trait CompilerFlag extends Flag
  object CompilerFlag {
    case object BufferWarnings extends CompilerFlag
    case object CompressDebugSections extends CompilerFlag
    case object Debug extends CompilerFlag
    case object FatalWarnings extends CompilerFlag
    case object FunctionLinking extends CompilerFlag
    case object NoExecStack extends CompilerFlag
    case object SecurityWarnings extends CompilerFlag
    object CallingConvention {
      case object CDecl extends CompilerFlag
    }
    object CppExtensions {
      case object Inline extends CompilerFlag
      case object WcharBuiltin extends CompilerFlag
    }
    object Exceptions {
      case object NoExtern extends CompilerFlag
      case object StandardCpp extends CompilerFlag
    }
    object FloatingPoint {
      case object Precise extends CompilerFlag
    }
    object LanguageStandard {
      case object Cpp11 extends CompilerFlag
    }
    object Linking {
      case object Multithreaded extends CompilerFlag
      case object MultithreadedExecutable extends CompilerFlag
    }
    object Optimizations {
      case object Fast extends CompilerFlag
      case object Intrinsics extends CompilerFlag
      case object WholeProgram extends CompilerFlag
    }
  }

  sealed trait LinkerFlag extends Flag
  object LinkerFlag {
    case object Debug extends LinkerFlag
    case object NonIncremental extends LinkerFlag
    object Manifest {
      case object Embedded extends LinkerFlag
    }
    object Optimizations {
      case object FoldDuplicates extends LinkerFlag
      case object RemoveUnreferenced extends LinkerFlag
      object WholeProgram {
        case object Incremental extends LinkerFlag
      }
    }
    object Subsystem {
      case object Windows extends LinkerFlag
    }
  }

  val empty: CppConfiguration = CppConfiguration("none", None, None)
}
