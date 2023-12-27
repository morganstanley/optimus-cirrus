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

import optimus.buildtool.config.CppConfiguration.CompilerFlag
import optimus.buildtool.config.CppConfiguration.LinkerFlag
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.PathUtils.{fingerprintAsset => fa}

import java.nio.file.Paths
import scala.collection.compat._
import scala.collection.immutable.Seq

object CppToolchain {
  val NoToolchain: CppToolchain = CppToolchain(
    name = "none",
    tpe = "None",
    compiler = FileAsset(Paths.get("//no/toolchain")),
    compilerPath = Nil,
    compilerFlags = Set.empty,
    symbols = Nil,
    includes = Nil,
    None,
    compilerArgs = Nil,
    linker = FileAsset(Paths.get("//no/toolchain")),
    linkerPath = Nil,
    linkerFlags = Set.empty,
    libPath = Nil,
    linkerArgs = Nil
  )
}

final case class CppToolchain(
    name: String,
    tpe: String,
    compiler: FileAsset,
    compilerPath: Seq[Directory],
    compilerFlags: Set[CompilerFlag],
    symbols: Seq[String],
    includes: Seq[Directory],
    warningLevel: Option[Int], // 0 to 4
    compilerArgs: Seq[String],
    linker: FileAsset,
    linkerPath: Seq[Directory],
    linkerFlags: Set[LinkerFlag],
    libPath: Seq[Directory],
    linkerArgs: Seq[String]
) {

  def fingerprint: Seq[String] = {
    Seq(s"[Toolchain]$tpe", fa("Toolchain:Compiler", compiler)) ++
      compilerPath.map(fa("Toolchain:CompilerPath", _)) ++
      compilerFlags.map(f => s"[Toolchain:CompilerFlag]${f.getClass.getName}").to(Seq).sorted ++
      symbols.map(s => s"[Toolchain:Symbol]$s") ++
      includes.map(fa("Toolchain:Include", _)) ++
      warningLevel.map(l => s"[Toolchain:WarningLevel]$l") ++
      compilerArgs.map(a => s"[Toolchain:CompilerArg]$a") ++
      Seq(fa("Toolchain:Linker", linker)) ++
      linkerPath.map(fa("Toolchain:LinkerPath", _, strict = false)) ++ // VC has some odd xml files in its libpath
      linkerFlags.map(f => s"[Toolchain:LinkerFlag]${f.getClass.getName}").to(Seq).sorted ++
      libPath.map(fa("Toolchain:LibPath", _)) ++
      linkerArgs.map(a => s"[Toolchain:LinkerArg]$a")
  }

}
