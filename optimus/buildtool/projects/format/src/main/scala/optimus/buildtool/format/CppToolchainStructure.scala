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
package optimus.buildtool.format

import com.typesafe.config.Config
import com.typesafe.config.ConfigUtil
import optimus.buildtool.config.CppConfiguration.CompilerFlag
import optimus.buildtool.config.CppConfiguration.Flag
import optimus.buildtool.config.CppConfiguration.LinkerFlag
import optimus.buildtool.config.CppToolchain
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.utils.PathUtils

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.reflect.runtime.universe._

final case class CppToolchainStructure(toolchains: Set[CppToolchain])

object CppToolchainStructure {
  private val origin = CppToolchainConfig

  // Note: CppToolchainStructure always contains 'NoToolchain' at minimum
  def load(loader: ObtFile.Loader): Result[CppToolchainStructure] = loader(origin).flatMap { config =>
    if (config.hasPath("toolchains")) {
      val toolchainsConfig = config.getConfig("toolchains")
      Result.tryWith(origin, toolchainsConfig) {
        Result
          .sequence(
            toolchainsConfig.keySet
              .map { name =>
                loadToolchain(name, toolchainsConfig.getConfig(ConfigUtil.quoteString(name)))
              }
              .to(Seq))
          .map(tc => CppToolchainStructure(tc.toSet + CppToolchain.NoToolchain))

      }
    } else Success(CppToolchainStructure(Set(CppToolchain.NoToolchain)))
  }

  private def loadToolchain(name: String, cfg: Config): Result[CppToolchain] = {
    Success(
      CppToolchain(
        name = name,
        tpe = cfg.getString("type"),
        compiler = FileAsset(PathUtils.get(cfg.getString("compiler"))),
        compilerPath = cfg.stringListOrEmpty("compilerPath").map(p => Directory(PathUtils.get(p))),
        compilerFlags = cfg.stringListOrEmpty("compilerFlags").map(parseCompilerFlag).toSet,
        symbols = cfg.stringListOrEmpty("symbols"),
        warningLevel = cfg.optionalString("warningLevel").map(_.toInt),
        includes = cfg.stringListOrEmpty("includes").map(p => Directory(PathUtils.get(p))),
        compilerArgs = cfg.stringListOrEmpty("compilerArgs"),
        linker = FileAsset(PathUtils.get(cfg.getString("linker"))),
        linkerPath = cfg.stringListOrEmpty("linkerPath").map(p => Directory(PathUtils.get(p))),
        linkerFlags = cfg.stringListOrEmpty("linkerFlags").map(parseLinkerFlag).toSet,
        libPath = cfg.stringListOrEmpty("libPath").map(p => Directory(PathUtils.get(p))),
        linkerArgs = cfg.stringListOrEmpty("linkerArgs")
      )
    )
      .withProblems(cfg.checkExtraProperties(origin, Keys.cppToolchain))
  }

  // Getting runtime mirror is expensive! Here is ok cause we do it only once since we are inside an object
  private val mirror = runtimeMirror(getClass.getClassLoader)
  private def parseFlag[A <: Flag](prefix: String, flagStr: String): A = {
    // calling-convention.c-decl => CallingConvention.CDecl
    val objName = s"$prefix.${NamingConventions.convert(flagStr, Seq('.' -> ".", '-' -> ""))}"
    // Consider refactoring to use Java reflection (see RuntimeMirror.moduleByName) from 2.13
    val obj = mirror.staticModule(objName)
    mirror.reflectModule(obj).instance.asInstanceOf[A]
  }

  private def printFlag[A <: Flag](prefix: String, f: A): String = {
    // CallingConvention.CDecl => calling-convention.c-decl
    val classStyleName = f.getClass.getName.stripPrefix(s"$prefix$$").stripSuffix("$")
    classStyleName.replace('$', '.').replaceAll("""([\w])([A-Z])""", "$1-$2").toLowerCase
  }

  private val rawCompilerFlagClass = classOf[CompilerFlag].getName
  private val compilerFlagClass = rawCompilerFlagClass.replace('$', '.')
  def parseCompilerFlag(flagStr: String): CompilerFlag = parseFlag[CompilerFlag](compilerFlagClass, flagStr)
  def printCompilerFlag(flag: CompilerFlag): String = printFlag[CompilerFlag](rawCompilerFlagClass, flag)

  private val rawLinkerFlagClass = classOf[LinkerFlag].getName
  private val linkerFlagClass = rawLinkerFlagClass.replace('$', '.')
  def parseLinkerFlag(flagStr: String): LinkerFlag = parseFlag[LinkerFlag](linkerFlagClass, flagStr)
  def printLinkerFlag(flag: LinkerFlag): String = printFlag[LinkerFlag](rawLinkerFlagClass, flag)

}
